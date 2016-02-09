/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.network.protocol;

import alluxio.Constants;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    RPCMessage.Type type = RPCMessage.Type.decode(in);
    RPCMessage message = RPCMessage.decodeMessage(type, in);
    out.add(message);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Error in decoding message. Possible Client/DataServer version incompatibility: "
        + cause.getMessage());
    // Return an error message to the client.
    ctx.channel()
        .writeAndFlush(new RPCErrorResponse(RPCResponse.Status.DECODE_ERROR))
        .addListener(ChannelFutureListener.CLOSE);
  }
}
