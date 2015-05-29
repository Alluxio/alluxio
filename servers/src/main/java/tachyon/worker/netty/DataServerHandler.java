/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockHandler;
import tachyon.worker.BlocksLocker;
import tachyon.worker.netty.protocol.RPCBlockRequest;
import tachyon.worker.netty.protocol.RPCBlockResponse;
import tachyon.worker.netty.protocol.RPCMessage;
import tachyon.worker.netty.protocol.buffer.DataBuffer;
import tachyon.worker.netty.protocol.buffer.DataByteBuffer;
import tachyon.worker.netty.protocol.buffer.DataFileChannel;
import tachyon.worker.tiered.StorageDir;

/**
 * This class has the main logic of the read path to process
 * {@link tachyon.worker.netty.protocol.RPCRequest} messages and return
 * {@link tachyon.worker.netty.protocol.RPCResponse} messages.
 */
@ChannelHandler.Sharable
public final class DataServerHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlocksLocker mLocker;
  private final TachyonConf mTachyonConf;
  private final FileTransferType mTransferType;

  public DataServerHandler(final BlocksLocker locker, TachyonConf tachyonConf) {
    mLocker = locker;
    mTachyonConf = tachyonConf;
    mTransferType =
        mTachyonConf.getEnum(Constants.WORKER_NETTY_FILE_TRANSFER_TYPE, FileTransferType.TRANSFER);
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException {
    switch (msg.getType()) {
      case RPC_BLOCK_REQUEST:
        handleBlockRequest(ctx, (RPCBlockRequest) msg);
        break;
      default:
        throw new IllegalArgumentException("No handler implementation for rpc msg type: "
            + msg.getType());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }

  private void handleBlockRequest(final ChannelHandlerContext ctx, final RPCBlockRequest req)
      throws IOException {
    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long len = req.getLength();
    final int lockId = mLocker.getLockId();
    final StorageDir storageDir = mLocker.lock(blockId, lockId);

    BlockHandler handler = null;
    try {
      req.validate();
      handler = storageDir.getBlockHandler(blockId);

      final long fileLength = handler.getLength();
      validateBounds(req, fileLength);
      final long readLength = returnLength(offset, len, fileLength);
      ChannelFuture future =
          ctx.writeAndFlush(new RPCBlockResponse(blockId, offset, readLength, getDataBuffer(req,
              handler, readLength)));
      future.addListener(ChannelFutureListener.CLOSE);
      future.addListener(new ClosableResourceChannelListener(handler));
      storageDir.accessBlock(blockId);
      LOG.info("Response remote request by reading from {}, preparation done.",
          storageDir.getBlockFilePath(blockId));
    } catch (Exception e) {
      // TODO This is a trick for now. The data may have been removed before remote retrieving.
      LOG.error("The file is not here : " + e.getMessage(), e);
      RPCBlockResponse resp = RPCBlockResponse.createErrorResponse(blockId);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (handler != null) {
        handler.close();
      }
    } finally {
      mLocker.unlock(blockId, lockId);
    }
  }

  /**
   * Returns how much of a file to read. When {@code len} is {@code -1}, then
   * {@code fileLength - offset} is used.
   */
  private long returnLength(final long offset, final long len, final long fileLength) {
    return (len == -1) ? fileLength - offset : len;
  }

  private void validateBounds(final RPCBlockRequest req, final long fileLength) {
    Preconditions.checkArgument(req.getOffset() <= fileLength,
        "Offset(%s) is larger than file length(%s)", req.getOffset(), fileLength);
    Preconditions.checkArgument(req.getLength() == -1
        || req.getOffset() + req.getLength() <= fileLength,
        "Offset(%s) plus length(%s) is larger than file length(%s)", req.getOffset(),
        req.getLength(), fileLength);
  }



  /**
   * Returns the appropriate DataBuffer representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param req The initiating RPCBlockRequest
   * @param handler The BlockHandler for the block to read
   * @param readLength The length, in bytes, of the data to read from the block
   * @return a DataBuffer representing the data
   * @throws IOException
   * @throws IllegalArgumentException
   */
  private DataBuffer getDataBuffer(RPCBlockRequest req, BlockHandler handler, long readLength)
      throws IOException, IllegalArgumentException {
    switch (mTransferType) {
      case MAPPED:
        ByteBuffer data = handler.read(req.getOffset(), (int) readLength);
        return new DataByteBuffer(data, readLength);
      case TRANSFER: // intend to fall through as TRANSFER is the default type.
      default:
        if (handler.getChannel() instanceof FileChannel) {
          return new DataFileChannel((FileChannel) handler.getChannel(), req.getOffset(),
              readLength);
        }
        handler.close();
        throw new IllegalArgumentException("Only FileChannel is supported!");
    }
  }
}
