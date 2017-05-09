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

import alluxio.RpcUtils;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.IdUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockWorker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Netty handler that handles short circuit read requests.
 *
 * This handler is associates any resources such as locks or temporary blocks with the same
 * session id and will clean up these resources if the channel is closed.
 */
@NotThreadSafe
class DataServerShortCircuitReadHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataServerShortCircuitReadHandler.class);

  /** The block worker. */
  private final BlockWorker mBlockWorker;
  /** The session id of this handler. */
  private final long mSessionId;
  /** The lock id of the block being read. */
  private long mLockId;

  /**
   * Creates an instance of {@link DataServerShortCircuitReadHandler}.
   *
   * @param blockWorker the block worker
   */
  DataServerShortCircuitReadHandler(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
    mSessionId = IdUtils.getRandomNonNegativeLong();
    mLockId = BlockLockManager.INVALID_LOCK_ID;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof RPCProtoMessage)) {
      ctx.fireChannelRead(msg);
      return;
    }

    ProtoMessage message = ((RPCProtoMessage) msg).getMessage();
    if (message.isLocalBlockOpenRequest()) {
      handleBlockOpenRequest(ctx, message.asLocalBlockOpenRequest());
    } else if (message.isLocalBlockCloseRequest()) {
      handleBlockCloseRequest(ctx, message.asLocalBlockCloseRequest());
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
    // The RPC handlers do not throw exceptions. All the exception seen here is either
    // network exception or some runtime exception (e.g. NullPointerException).
    LOG.error("Failed to handle RPCs.", throwable);
    ctx.close();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
      try {
        mBlockWorker.unlockBlock(mLockId);
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to unlock lock {} with error {}.", mLockId, e.getMessage());
      }
    }
    // Currently this is a no-op since there are no temporary blocks held by this handler. This
    // is here for consistency.
    mBlockWorker.cleanupSession(mSessionId);
  }

  /**
   * Handles {@link Protocol.LocalBlockOpenRequest}. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block open request
   */
  private void handleBlockOpenRequest(final ChannelHandlerContext ctx,
      final Protocol.LocalBlockOpenRequest request) {
    RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRPCCallable<Void>() {

      @Override
      public Void call() throws Exception {
        // It is a no-op to lock the same block multiple times within the same channel.
        if (mLockId == BlockLockManager.INVALID_LOCK_ID) {
          mLockId = mBlockWorker.lockBlock(mSessionId, request.getBlockId());
        } else {
          LOG.warn("Lock block {} without releasing previous block lock {}.", request.getBlockId(),
              mLockId);
          throw new InvalidWorkerStateException(
              ExceptionMessage.LOCK_NOT_RELEASED.getMessage(mLockId));
        }
        Protocol.LocalBlockOpenResponse response = Protocol.LocalBlockOpenResponse.newBuilder()
            .setPath(mBlockWorker.readBlock(mSessionId, request.getBlockId(), mLockId))
            .build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response)));

        return null;
      }

      @Override
      public void exceptionCaught(Throwable e) {
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

      @Override
      public String toString() {
        return String.format("Open block: %s", request.toString());
      }
    });
  }

  /**
   * Handles {@link Protocol.LocalBlockCloseRequest}. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block close request
   */
  private void handleBlockCloseRequest(final ChannelHandlerContext ctx,
      final Protocol.LocalBlockCloseRequest request) {
    RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRPCCallable<Void>() {

      @Override
      public Void call() throws Exception {
        if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
          mBlockWorker.unlockBlock(mLockId);
          mLockId = BlockLockManager.INVALID_LOCK_ID;
        } else {
          LOG.warn("Close a closed block {}.", request.getBlockId());
        }
        ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
        return null;
      }

      @Override
      public void exceptionCaught(Throwable e) {
        ctx.writeAndFlush(RPCProtoMessage.createResponse(AlluxioStatusException.from(e)));
        mLockId = BlockLockManager.INVALID_LOCK_ID;
      }

      @Override
      public String toString() {
        return String.format("Close block: %s", request.toString());
      }
    });
  }
}
