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

package alluxio.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Simple Netty decoder which converts the input ByteBuf into an RPCMessage.
 * The frame decoder should have already run earlier in the Netty pipeline, and split up the stream
 * into individual encoded messages.
 */
@ChannelHandler.Sharable
@ThreadSafe
public final class RPCMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
  private static final Logger LOG = LoggerFactory.getLogger(RPCMessageDecoder.class);

  /**
   * Constructs a new {@link RPCMessageDecoder}.
   */
  public RPCMessageDecoder() {}

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    RPCMessage.Type type = RPCMessage.Type.decode(in);
    RPCMessage message = RPCMessage.decodeMessage(type, in);
    out.add(message);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Error in decoding message.", cause);
    ctx.close();
  }
}
