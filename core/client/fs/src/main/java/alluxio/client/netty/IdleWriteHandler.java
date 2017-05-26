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

package alluxio.client.netty;

import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Sends a keep-alive to the server whenever the channel has been idle for a period of time.
 */
public class IdleWriteHandler extends ChannelDuplexHandler {
  /**
   * Creates a new idle write handler.
   */
  public IdleWriteHandler() {}

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      if (((IdleStateEvent) evt).state() == IdleState.WRITER_IDLE) {
        Protocol.Heartbeat heartbeat = Protocol.Heartbeat.newBuilder().build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(heartbeat)));
      }
    }
  }
}
