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

import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.AsyncCacheRequestManager;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler for async cache requests.
 */
public class AsyncCacheHandler extends ChannelInboundHandlerAdapter {
  private final AsyncCacheRequestManager mRequestManager;

  /**
   * Constructs a new async cache handler.
   */
  public AsyncCacheHandler() {
    mRequestManager = new AsyncCacheRequestManager();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (object instanceof RPCProtoMessage
        && ((RPCProtoMessage) object).getMessage().isAsyncCacheRequest()) {
      Protocol.AsyncCacheRequest request =
          ((RPCProtoMessage) object).getMessage().asAsyncCacheRequest();

      // TODO(calvin): The worker net address should be a proto object and passed over the wire
      mRequestManager.submitRequest(
          request.getBlockId(),
          new WorkerNetAddress().setHost(request.getSourceHost()).setDataPort(
              request.getSourcePort()));
      ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
    } else {
      ctx.fireChannelRead(object);
    }
  }
}
