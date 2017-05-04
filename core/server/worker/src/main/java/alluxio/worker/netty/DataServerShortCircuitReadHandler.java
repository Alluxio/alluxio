/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.netty;

import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Netty handler that handles short circuit read requests.
 */
@NotThreadSafe
class DataServerShortCircuitReadHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerReadHandler.class);

  /** The block worker. */
  private final BlockWorker mBlockWorker;
  /** The lock Id of the block being read. */
  private long mLockId;

  /**
   * Creates an instance of {@link DataServerShortCircuitReadHandler}.
   *
   * @param blockWorker the block worker
   */
  DataServerShortCircuitReadHandler(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
    mLockId = BlockLockManager.INVALID_LOCK_ID;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!acceptMessage(msg)) {
      ctx.fireChannelRead(msg);
      return;
    }

    ProtoMessage message = ((RPCProtoMessage) msg).getMessage();
    if (message.isLocalBlockCloseRequest()) {
      handleBlockOpenRequest(ctx, message.asLocalBlockOpenRequest());
    } else {
      Preconditions.checkState(message.isLocalBlockCloseRequest());
      handleBlockCloseRequest(ctx, message.asLocalBlockCloseRequest());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
    // The RPC handlers do not throw exceptions. All the exception seen here is either
    // network exception or some runtime exception (e.g. NullPointerException).
    LOG.error("Failed to handle RPCs.", throwable);
    ctx.close();
  }

  private boolean acceptMessage(Object msg) {
    return (msg instanceof RPCProtoMessage) && (
        ((RPCProtoMessage) msg).getMessage().isLocalBlockOpenRequest() ||
            ((RPCProtoMessage) msg).getMessage().isLocalBlockCloseRequest());
  }

  /**
   * Handles {@link Protocol.LocalBlockOpenRequest}. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block open request
   */
  void handleBlockOpenRequest(ChannelHandlerContext ctx, Protocol.LocalBlockOpenRequest request) {
    // It is a no-op to lock the same block multiple times within the same channel.
    if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
      try {
        mLockId = mBlockWorker.lockBlock(request.getSessionId(), request.getBlockId());
        Protocol.LocalBlockOpenResponse response = Protocol.LocalBlockOpenResponse.newBuilder()
            .setPath(mBlockWorker.readBlock(request.getSessionId(), request.getBlockId(), mLockId))
            .build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response)));
      } catch (BlockDoesNotExistException | InvalidWorkerStateException e) {
        if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
          try {
            mBlockWorker.unlockBlock(mLockId);
          } catch (BlockDoesNotExistException ee) {
            LOG.error("Failed to unlock block {}.", request.getBlockId(), e);
          }
          mLockId = BlockLockManager.INVALID_LOCK_ID;
        }
        ctx.writeAndFlush(RPCProtoMessage.createResponse(AlluxioStatusException.from(e)));
      }
    }
  }

  /**
   * Handles {@link Protocol.LocalBlockCloseRequest}. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block close request
   */
  void handleBlockCloseRequest(ChannelHandlerContext ctx, Protocol.LocalBlockCloseRequest request) {
    if (mLockId == BlockLockManager.INVALID_LOCK_ID) {
      LOG.warn("Close a block {} without holding the lock.", request.getBlockId());
      // It is an no-op to close a block if the block is not locked.
      return;
    }
    try {
      mBlockWorker.unlockBlock(mLockId);
      ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
    } catch (BlockDoesNotExistException e) {
      ctx.writeAndFlush(RPCProtoMessage.createResponse(AlluxioStatusException.from(e)));
    } finally {
      mLockId = BlockLockManager.INVALID_LOCK_ID;
    }
  }
}
