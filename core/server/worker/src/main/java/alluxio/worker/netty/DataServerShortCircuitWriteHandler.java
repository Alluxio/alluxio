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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.IdUtils;
import alluxio.util.proto.ProtoMessage;
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
class DataServerShortCircuitWriteHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataServerShortCircuitWriteHandler.class);

  private static final long INVALID_SESSION_ID = -1;

  /** Executor service for execute the RPCs. */
  private final ExecutorService mRpcExecutor;
  /** The block worker. */
  private final BlockWorker mBlockWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  private long mSessionId = INVALID_SESSION_ID;

  /**
   * Creates an instance of {@link DataServerShortCircuitWriteHandler}.
   *
   * @param service the executor to execute the RPCs
   * @param blockWorker the block worker
   */
  DataServerShortCircuitWriteHandler(ExecutorService service, BlockWorker blockWorker) {
    mRpcExecutor = service;
    mBlockWorker = blockWorker;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof RPCProtoMessage)) {
      ctx.fireChannelRead(msg);
      return;
    }

    ProtoMessage message = ((RPCProtoMessage) msg).getMessage();
    if (message.isLocalBlockCreateRequest()) {
      handleBlockCreateRequest(ctx, message.asLocalBlockCreateRequest());
    } else if (message.isLocalBlockCompleteRequest()) {
      handleBlockCompleteRequest(ctx, message.asLocalBlockCompleteRequest());
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
    if (mSessionId != INVALID_SESSION_ID) {
      mBlockWorker.cleanupSession(mSessionId);
      mSessionId = INVALID_SESSION_ID;
    }
    ctx.fireChannelUnregistered();
  }

  /**
   * Handles {@link Protocol.LocalBlockCreateRequest} to create block. No exceptions should be
   * thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block create request
   */
  private void handleBlockCreateRequest(final ChannelHandlerContext ctx,
      final Protocol.LocalBlockCreateRequest request) {
    mRpcExecutor.submit(new Runnable() {
      @Override
      public void run() {
        RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRPCCallable<Void>() {

          @Override
          public Void call() throws Exception {
            if (request.getOnlyReserveSpace()) {
              mBlockWorker
                  .requestSpace(mSessionId, request.getBlockId(), request.getSpaceToReserve());
              ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
            } else {
              if (mSessionId == INVALID_SESSION_ID) {
                mSessionId = IdUtils.createSessionId();
                String path = mBlockWorker.createBlock(mSessionId, request.getBlockId(),
                    mStorageTierAssoc.getAlias(request.getTier()), request.getSpaceToReserve());
                Protocol.LocalBlockCreateResponse response =
                    Protocol.LocalBlockCreateResponse.newBuilder().setPath(path).build();
                ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response)));
              } else {
                LOG.warn("Create block {} without closing the previous session {}.",
                    request.getBlockId(), mSessionId);
                throw new InvalidWorkerStateException(
                    ExceptionMessage.SESSION_NOT_CLOSED.getMessage(mSessionId));
              }
            }
            return null;
          }

          @Override
          public void exceptionCaught(Throwable throwable) {
            if (mSessionId != INVALID_SESSION_ID) {
              mBlockWorker.cleanupSession(mSessionId);
              mSessionId = INVALID_SESSION_ID;
            }
            ctx.writeAndFlush(
                RPCProtoMessage.createResponse(AlluxioStatusException.fromThrowable(throwable)));
          }

          @Override
          public String toString() {
            if (request.getOnlyReserveSpace()) {
              return String.format("Session %d: reserve space: %s", mSessionId, request.toString());
            } else {
              return String.format("Session %d: create block: %s", mSessionId, request.toString());
            }
          }
        });
      }
    });
  }

  /**
   * Handles {@link Protocol.LocalBlockCompleteRequest}. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block close request
   */
  private void handleBlockCompleteRequest(final ChannelHandlerContext ctx,
      final Protocol.LocalBlockCompleteRequest request) {
    mRpcExecutor.submit(new Runnable() {
      @Override
      public void run() {

        RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRPCCallable<Void>() {

          @Override
          public Void call() throws Exception {
            if (request.getCancel()) {
              mBlockWorker.abortBlock(mSessionId, request.getBlockId());
            } else {
              mBlockWorker.commitBlock(mSessionId, request.getBlockId());
            }
            mSessionId = INVALID_SESSION_ID;
            ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
            return null;
          }

          @Override
          public void exceptionCaught(Throwable throwable) {
            ctx.writeAndFlush(
                RPCProtoMessage.createResponse(AlluxioStatusException.fromThrowable(throwable)));
            mSessionId = INVALID_SESSION_ID;
          }

          @Override
          public String toString() {
            if (request.getCancel()) {
              return String.format("Session %d: abort block: %s", mSessionId, request.toString());
            } else {
              return String.format("Session %d: commit block: %s", mSessionId, request.toString());
            }
          }
        });
      }
    });
  }
}
