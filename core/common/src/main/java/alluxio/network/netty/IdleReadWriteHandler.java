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

package alluxio.network.netty;

import alluxio.PropertyKey;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a keep-alive to the server whenever the channel has been idle for a period of time.
 */
public class IdleReadWriteHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IdleReadWriteHandler.class);

  /**
   * Creates a new idle read write handler.
   */
  public IdleReadWriteHandler() {}

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof IdleStateEvent) {
      IdleState state = ((IdleStateEvent) evt).state();
      if (state == IdleState.WRITER_IDLE) {
        Protocol.Heartbeat heartbeat = Protocol.Heartbeat.newBuilder().build();
        ctx.writeAndFlush(new RPCProtoMessage(new ProtoMessage(heartbeat)));
      } else if (state == IdleState.READER_IDLE) {
        LOG.info("Netty reader is idle more than {}, closing context.",
            PropertyKey.Name.NETWORK_NETTY_READ_IDLE_TIMEOUT_MS);
        ctx.close();
      }
    }
  }
}
