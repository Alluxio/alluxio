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

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.BlockWorker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Netty handler that handles short circuit read requests.
 */
@NotThreadSafe
class DataServerShortCircuitWriteHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataServerShortCircuitWriteHandler.class);

  /** The block worker. */
  private final BlockWorker mBlockWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /**
   * Creates an instance of {@link DataServerShortCircuitWriteHandler}.
   *
   * @param blockWorker the block worker
   */
  DataServerShortCircuitWriteHandler(BlockWorker blockWorker) {
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
      if (message.asLocalBlockCreateRequest().getOnlyReserveSpace()) {
        handleReserveSpaceRequest(ctx, message.asLocalBlockCreateRequest());
      } else {
        handleBlockCreateRequest(ctx, message.asLocalBlockCreateRequest());
      }
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

  /**
   * Handles {@link Protocol.LocalBlockCreateRequest} to create block. No exceptions should be
   * thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block create request
   */
  private void handleBlockCreateRequest(ChannelHandlerContext ctx,
      Protocol.LocalBlockCreateRequest request) {
    LOG.debug("Create block: {}.", request);
    try {
      String path = mBlockWorker.createBlock(request.getSessionId(), request.getBlockId(),
          mStorageTierAssoc.getAlias(request.getTier()), request.getSpaceToReserve());
      Protocol.LocalBlockCreateResponse response =
          Protocol.LocalBlockCreateResponse.newBuilder().setPath(path).build();
      ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(response)));
    } catch (Exception e) {
      ctx.writeAndFlush(RPCProtoMessage.createResponse(AlluxioStatusException.from(e)));
    }
  }

  /**
   * Handles {@link Protocol.LocalBlockCreateRequest} to reserve space only. No exceptions should
   * be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block create request
   */
  private void handleReserveSpaceRequest(ChannelHandlerContext ctx,
      Protocol.LocalBlockCreateRequest request) {
    LOG.debug("Reserve space: {}.", request);
    try {
      mBlockWorker
          .requestSpace(request.getSessionId(), request.getBlockId(), request.getSpaceToReserve());
      ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
    } catch (Exception e) {
      ctx.writeAndFlush(RPCProtoMessage.createResponse(AlluxioStatusException.from(e)));
    }
  }

  /**
   * Handles {@link Protocol.LocalBlockCompleteRequest}. No exceptions should be thrown.
   *
   * @param ctx the channel handler context
   * @param request the local block close request
   */
  private void handleBlockCompleteRequest(ChannelHandlerContext ctx,
      Protocol.LocalBlockCompleteRequest request) {
    try {
      if (request.getCancel()) {
        LOG.debug("Abort block: {}.", request);
        mBlockWorker.abortBlock(request.getSessionId(), request.getBlockId());
      } else {
        LOG.debug("Commit block: {}.", request);
        mBlockWorker.commitBlock(request.getSessionId(), request.getBlockId());
      }
      ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
    } catch (Exception e) {
      ctx.writeAndFlush(RPCProtoMessage.createResponse(AlluxioStatusException.from(e)));
    }
  }
}
