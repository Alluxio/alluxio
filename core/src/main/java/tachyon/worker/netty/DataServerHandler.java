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


import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.worker.BlockHandler;
import tachyon.worker.BlocksLocker;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Main logic for the read path. This class consumes {@link tachyon.worker.netty.BlockRequest}
 * messages and returns {@link tachyon.worker.netty.BlockResponse} messages.
 */
@ChannelHandler.Sharable
public final class DataServerHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlocksLocker mLocker;

  public DataServerHandler(BlocksLocker locker) {
    mLocker = locker;
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    // pipeline will make sure this is true
    final BlockRequest req = (BlockRequest) msg;

    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long len = req.getLength();
    final int lockId = mLocker.getLockId();
    final StorageDir storageDir = mLocker.lock(blockId, lockId);

    BlockHandler handler = null;
    try {
      validateInput(req);
      handler = storageDir.getBlockHandler(blockId);

      final long fileLength = handler.getLength();
      validateBounds(req, fileLength);
      final long readLength = returnLength(offset, len, fileLength);
      ChannelFuture future =
          ctx.writeAndFlush(new BlockResponse(blockId, offset, readLength, handler));
      future.addListener(ChannelFutureListener.CLOSE);
      future.addListener(new ClosableResourceChannelListener(handler));
      storageDir.accessBlock(blockId);
      LOG.info("Response remote request by reading from {}, preparation done.",
          storageDir.getBlockFilePath(blockId));
    } catch (Exception e) {
      // TODO This is a trick for now. The data may have been removed before remote retrieving.
      LOG.error("The file is not here : " + e.getMessage(), e);
      BlockResponse resp = BlockResponse.createErrorResponse(blockId);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (handler != null) {
        handler.close();
      }
    } finally {
      mLocker.unlock(blockId, lockId);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }

  /**
   * Returns how much of a file to read. When {@code len} is {@code -1}, then
   * {@code fileLength - offset} is used.
   */
  private long returnLength(final long offset, final long len, final long fileLength) {
    if (len == -1) {
      return fileLength - offset;
    } else {
      return len;
    }
  }

  private void validateBounds(final BlockRequest req, final long fileLength) {
    if (req.getOffset() > fileLength) {
      String msg =
          String.format("Offset(%d) is larger than file length(%d)", req.getOffset(), fileLength);
      throw new IllegalArgumentException(msg);
    }
    if (req.getLength() != -1 && req.getOffset() + req.getLength() > fileLength) {
      String msg =
          String.format("Offset(%d) plus length(%d) is larger than file length(%d)",
              req.getOffset(), req.getLength(), fileLength);
      throw new IllegalArgumentException(msg);
    }
  }

  private void validateInput(final BlockRequest req) {
    if (req.getOffset() < 0) {
      throw new IllegalArgumentException("Offset can not be negative: " + req.getOffset());
    }
    if (req.getLength() < 0 && req.getLength() != -1) {
      String msg = "Length can not be negative except -1: " + req.getLength();
      throw new IllegalArgumentException(msg);
    }
  }
}
