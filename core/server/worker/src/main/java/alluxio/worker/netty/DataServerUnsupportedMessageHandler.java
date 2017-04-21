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

import alluxio.exception.status.UnimplementedException;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.RPCResponse;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Netty handler which replies to the client with an unknown message error for message types
 * which are no longer supported.
 */
@ChannelHandler.Sharable
@ThreadSafe
public class DataServerUnsupportedMessageHandler extends ChannelInboundHandlerAdapter {
  /**
   * Constructs a new unsupported message handler.
   */
  public DataServerUnsupportedMessageHandler() {}

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (object instanceof RPCProtoMessage) { // Unknown proto message, reply proto.
      RPCProtoMessage resp =
          RPCProtoMessage.createResponse(new UnimplementedException("Unrecognized RPC: " + object));
      ctx.writeAndFlush(resp);
    } else if (object instanceof RPCMessage) { // Unknown non-proto message, reply non-proto.
      RPCErrorResponse resp = new RPCErrorResponse(RPCResponse.Status.UNKNOWN_MESSAGE_ERROR);
      ctx.writeAndFlush(resp);
    } else { // Unknown message, this should not happen.
      ctx.fireChannelRead(object);
    }
  }
}
