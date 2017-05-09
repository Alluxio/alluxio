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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A handler for heartbeat events.
 */
public class DataServerHeartbeatHandler extends ChannelInboundHandlerAdapter {
  /**
   * Constructs a new heartbeat handler.
   */
  public DataServerHeartbeatHandler() {}

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (object instanceof RPCProtoMessage
        && ((RPCProtoMessage) object).getMessage().isHeartbeat()) {
      // do nothing
    } else {
      ctx.fireChannelRead(object);
    }
  }
}
