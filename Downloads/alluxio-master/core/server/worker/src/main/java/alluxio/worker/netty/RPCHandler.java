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
 * Netty handler that handles any stateless data server RPCs.
 */
@NotThreadSafe
class RPCHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(RPCHandler.class);

  /** Executor service for stateless RPCs. */
  private final ExecutorService mRPCExecutor;
  /** The block worker. */
  private final BlockWorker mWorker;

  /**
   * Creates an instance of {@link RPCHandler}.
   *
   * @param blockWorker the block worker
   */
  RPCHandler(ExecutorService service, BlockWorker blockWorker) {
    mRPCExecutor = service;
    mWorker = blockWorker;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof RPCProtoMessage)) {
      ctx.fireChannelRead(msg);
      return;
    }

    ProtoMessage message = ((RPCProtoMessage) msg).getMessage();
    if (message.isRemoveBlockRequest()) {
      handleRemoveBlockRequest(ctx, message.asRemoveBlockRequest());
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

  private void handleRemoveBlockRequest(final ChannelHandlerContext ctx,
      final Protocol.RemoveBlockRequest request) {
    mRPCExecutor.submit(() -> {
      final long sessionId = IdUtils.createSessionId();
      RpcUtils.nettyRPCAndLog(LOG, new RpcUtils.NettyRpcCallable<Void>() {

        @Override
        public Void call() throws Exception {
          mWorker.removeBlock(sessionId, request.getBlockId());
          ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
          return null;
        }

        @Override
        public void exceptionCaught(Throwable throwable) {
          ctx.writeAndFlush(
              RPCProtoMessage.createResponse(AlluxioStatusException.fromThrowable(throwable)));
          // This is actually not necessary since removeBlock should be able to
          // clean up any resources if it fails. Just to be safe.
          mWorker.cleanupSession(sessionId);
        }
      }, "HandleRemoveBlockRequest", "Request=%s", request);
    });
  }
}
