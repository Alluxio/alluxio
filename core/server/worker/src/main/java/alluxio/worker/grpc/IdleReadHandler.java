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

package alluxio.worker.grpc;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closes the channel if it has been idle for too long.
 */
public class IdleReadHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IdleReadHandler.class);

  /**
   * Creates a new idle read handler.
   */
  public IdleReadHandler() {}

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
        LOG.warn("Closing netty channel to {} due to inactivity.", ctx.channel().remoteAddress());
        ctx.close();
      }
    }
  }
}
