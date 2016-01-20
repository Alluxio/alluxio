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
import tachyon.StorageTierAssoc;
import tachyon.WorkerStorageTierAssoc;
import tachyon.conf.TachyonConf;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.network.protocol.RPCBlockReadRequest;
import tachyon.network.protocol.RPCBlockReadResponse;
import tachyon.network.protocol.RPCBlockWriteRequest;
import tachyon.network.protocol.RPCBlockWriteResponse;
import tachyon.network.protocol.RPCErrorResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCRequest;
import tachyon.network.protocol.RPCResponse;
import tachyon.network.protocol.databuffer.DataBuffer;
import tachyon.network.protocol.databuffer.DataByteBuffer;
import tachyon.network.protocol.databuffer.DataFileChannel;
import tachyon.worker.block.BlockWorker;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;

/**
 * This class has the main logic of the read path to process {@link RPCRequest} messages and return
 * {@link RPCResponse} messages.
 */
@ChannelHandler.Sharable
public final class DataServerHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlockWorker mBlockWorker;
  private final TachyonConf mTachyonConf;
  private final StorageTierAssoc mStorageTierAssoc;
  private final FileTransferType mTransferType;

  /**
   * Creates a new instance of {@link DataServerHandler}.
   *
   * @param dataManager a block data manager handle
   * @param tachyonConf Tachyon configuration
   */
  public DataServerHandler(final BlockWorker blockWorker, TachyonConf tachyonConf) {
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mStorageTierAssoc = new WorkerStorageTierAssoc(mTachyonConf);
    mTransferType = mTachyonConf.getEnum(Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE,
        FileTransferType.class);
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException {
    switch (msg.getType()) {
      case RPC_BLOCK_READ_REQUEST:
        assert msg instanceof RPCBlockReadRequest;
        handleBlockReadRequest(ctx, (RPCBlockReadRequest) msg);
        break;
      case RPC_BLOCK_WRITE_REQUEST:
        assert msg instanceof RPCBlockWriteRequest;
        handleBlockWriteRequest(ctx, (RPCBlockWriteRequest) msg);
        break;
      default:
        RPCErrorResponse resp = new RPCErrorResponse(RPCResponse.Status.UNKNOWN_MESSAGE_ERROR);
        ctx.writeAndFlush(resp);
        throw new IllegalArgumentException(
            "No handler implementation for rpc msg type: " + msg.getType());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }

  private void handleBlockReadRequest(final ChannelHandlerContext ctx,
      final RPCBlockReadRequest req) throws IOException {
    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long len = req.getLength();
    final long lockId = req.getLockId();
    final long sessionId = req.getSessionId();

    BlockReader reader;
    try {
      reader = mBlockWorker.readBlockRemote(sessionId, blockId, lockId);
    } catch (BlockDoesNotExistException nfe) {
      throw new IOException(nfe);
    } catch (InvalidWorkerStateException fpe) {
      throw new IOException(fpe);
    }
    try {
      req.validate();
      final long fileLength = reader.getLength();
      validateBounds(req, fileLength);
      final long readLength = returnLength(offset, len, fileLength);
      RPCBlockReadResponse resp = new RPCBlockReadResponse(blockId, offset, readLength,
          getDataBuffer(req, reader, readLength), RPCResponse.Status.SUCCESS);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      future.addListener(new ClosableResourceChannelListener(reader));
      mBlockWorker.accessBlock(sessionId, blockId);
      LOG.info("Preparation for responding to remote block request for: {} done.", blockId);
    } catch (Exception e) {
      LOG.error("The file is not here : {}", e.getMessage(), e);
      RPCBlockReadResponse resp =
          RPCBlockReadResponse.createErrorResponse(req, RPCResponse.Status.FILE_DNE);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (reader != null) {
        reader.close();
      }
    }
  }

  // TODO(hy): This write request handler is very simple in order to be stateless. Therefore, the
  // block file is opened and closed for every request. If this is too slow, then this handler
  // should be optimized to keep state.
  private void handleBlockWriteRequest(final ChannelHandlerContext ctx,
      final RPCBlockWriteRequest req) throws IOException {
    final long sessionId = req.getSessionId();
    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long length = req.getLength();
    final DataBuffer data = req.getPayloadDataBuffer();

    BlockWriter writer = null;
    try {
      req.validate();
      ByteBuffer buffer = data.getReadOnlyByteBuffer();

      if (offset == 0) {
        // This is the first write to the block, so create the temp block file. The file will only
        // be created if the first write starts at offset 0. This allocates enough space for the
        // write.
        mBlockWorker.createBlockRemote(sessionId, blockId, mStorageTierAssoc.getAlias(0), length);
      } else {
        // Allocate enough space in the existing temporary block for the write.
        mBlockWorker.requestSpace(sessionId, blockId, length);
      }
      writer = mBlockWorker.getTempBlockWriterRemote(sessionId, blockId);
      writer.append(buffer);

      RPCBlockWriteResponse resp =
          new RPCBlockWriteResponse(sessionId, blockId, offset, length, RPCResponse.Status.SUCCESS);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      future.addListener(new ClosableResourceChannelListener(writer));
    } catch (Exception e) {
      LOG.error("Error writing remote block : {}", e.getMessage(), e);
      RPCBlockWriteResponse resp =
          RPCBlockWriteResponse.createErrorResponse(req, RPCResponse.Status.WRITE_ERROR);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (writer != null) {
        writer.close();
      }
    }
  }

  /**
   * Returns how much of a file to read. When {@code len} is {@code -1}, then
   * {@code fileLength - offset} is used.
   */
  private long returnLength(final long offset, final long len, final long fileLength) {
    return (len == -1) ? fileLength - offset : len;
  }

  private void validateBounds(final RPCBlockReadRequest req, final long fileLength) {
    Preconditions.checkArgument(req.getOffset() <= fileLength,
        "Offset(%s) is larger than file length(%s)", req.getOffset(), fileLength);
    Preconditions.checkArgument(
        req.getLength() == -1 || req.getOffset() + req.getLength() <= fileLength,
        "Offset(%s) plus length(%s) is larger than file length(%s)", req.getOffset(),
        req.getLength(), fileLength);
  }

  /**
   * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param req The initiating {@link RPCBlockReadRequest}
   * @param reader The {@link BlockReader} for the block to read
   * @param readLength The length, in bytes, of the data to read from the block
   * @return a {@link DataBuffer} representing the data
   * @throws IOException
   * @throws IllegalArgumentException
   */
  private DataBuffer getDataBuffer(RPCBlockReadRequest req, BlockReader reader, long readLength)
      throws IOException, IllegalArgumentException {
    switch (mTransferType) {
      case MAPPED:
        ByteBuffer data = reader.read(req.getOffset(), (int) readLength);
        return new DataByteBuffer(data, readLength);
      case TRANSFER: // intend to fall through as TRANSFER is the default type.
      default:
        if (reader.getChannel() instanceof FileChannel) {
          return new DataFileChannel((FileChannel) reader.getChannel(), req.getOffset(),
              readLength);
        }
        reader.close();
        throw new IllegalArgumentException("Only FileChannel is supported!");
    }
  }
}
