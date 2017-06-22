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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
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

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Netty handler that handles short circuit read requests.
 */
@NotThreadSafe
class DataServerShortCircuitReadHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataServerShortCircuitReadHandler.class);

  /** Executor service for execute the RPCs. */
  private final ExecutorService mRpcExecutor;
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  /** The block worker. */
  private final BlockWorker mWorker;
  /** The lock Id of the block being read. */
  private long mLockId;
  private long mSessionId;

  /**
   * Creates an instance of {@link DataServerShortCircuitReadHandler}.
   *
   * @param service the executor to execute the RPCs
   * @param blockWorker the block worker
   */
  DataServerShortCircuitReadHandler(ExecutorService service, BlockWorker blockWorker) {
    mRpcExecutor = service;
    mWorker = blockWorker;
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
        mWorker.unlockBlock(mLockId);
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to unlock lock {} with error {}.", mLockId, e.getMessage());
      }
      mWorker.cleanupSession(mSessionId);
    }
    ctx.fireChannelUnregistered();
  }

  /**
   * Handles {@link Protocol.LocalBlockOpenRequest}. Since the open can be expensive, the work is
   * delegated to a threadpool. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block open request
   */
  private void handleBlockOpenRequest(final ChannelHandlerContext ctx,
      final Protocol.LocalBlockOpenRequest request) {
    mRpcExecutor.submit(new Runnable() {
      @Override
      public void run() {
        RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRPCCallable<Void>() {
          @Override
          public Void call() throws Exception {
            if (mLockId == BlockLockManager.INVALID_LOCK_ID) {
              mSessionId = IdUtils.createSessionId();
              // TODO(calvin): Update the locking logic so this can be done better
              if (request.getPromote()) {
                try {
                  mWorker
                      .moveBlock(mSessionId, request.getBlockId(), mStorageTierAssoc.getAlias(0));
                } catch (BlockDoesNotExistException e) {
                  LOG.debug("Block {} to promote does not exist in Alluxio: {}",
                      request.getBlockId(), e.getMessage());
                } catch (Exception e) {
                  LOG.warn("Failed to promote block {}: {}", request.getBlockId(), e.getMessage());
                }
              }
              mLockId = mWorker.lockBlock(mSessionId, request.getBlockId());
              mWorker.accessBlock(mSessionId, request.getBlockId());
            } else {
              LOG.warn("Lock block {} without releasing previous block lock {}.",
                  request.getBlockId(), mLockId);
              throw new InvalidWorkerStateException(
                  ExceptionMessage.LOCK_NOT_RELEASED.getMessage(mLockId));
            }
            Protocol.LocalBlockOpenResponse response = Protocol.LocalBlockOpenResponse.newBuilder()
                .setPath(mWorker.readBlock(mSessionId, request.getBlockId(), mLockId)).build();
            ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response)));
            return null;
          }

          @Override
          public void exceptionCaught(Throwable e) {
            if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
              try {
                mWorker.unlockBlock(mLockId);
              } catch (BlockDoesNotExistException ee) {
                LOG.error("Failed to unlock block {}.", request.getBlockId(), e);
              }
              mLockId = BlockLockManager.INVALID_LOCK_ID;
            }
            ctx.writeAndFlush(
                RPCProtoMessage.createResponse(AlluxioStatusException.fromThrowable(e)));
          }

          @Override
          public String toString() {
            return String.format("Session %d: open block: %s", mSessionId, request.toString());
          }
        });
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
    mRpcExecutor.submit(new Runnable() {
      @Override
      public void run() {

        RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRPCCallable<Void>() {

          @Override
          public Void call() throws Exception {
            if (mLockId != BlockLockManager.INVALID_LOCK_ID) {
              mWorker.unlockBlock(mLockId);
              mLockId = BlockLockManager.INVALID_LOCK_ID;
            } else {
              LOG.warn("Close a closed block {}.", request.getBlockId());
            }
            ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
            return null;
          }

          @Override
          public void exceptionCaught(Throwable e) {
            ctx.writeAndFlush(
                RPCProtoMessage.createResponse(AlluxioStatusException.fromThrowable(e)));
            mLockId = BlockLockManager.INVALID_LOCK_ID;
          }

          @Override
          public String toString() {
            return String.format("Session %d: close block: %s", mSessionId, request.toString());
          }
        });
      }
    });
  }
}
