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

import alluxio.network.protocol.databuffer.DataBuffer;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Simple Netty encoder for RPCMessages.
 */
@ChannelHandler.Sharable
@ThreadSafe
public final class RPCMessageEncoder extends MessageToMessageEncoder<RPCMessage> {

  @Override
  protected void encode(ChannelHandlerContext ctx, RPCMessage in, List<Object> out)
      throws Exception {
    RPCRequest.Type type = in.getType();

    long bodyBytes = 0;
    DataBuffer payload = null;

    if (in.hasPayload()) {
      payload = in.getPayloadDataBuffer();
      bodyBytes = payload.getLength();
    }

    int lengthBytes = Longs.BYTES;
    int typeBytes = type.getEncodedLength();
    int messageBytes = in.getEncodedLength();

    int headerBytes = lengthBytes + typeBytes + messageBytes;
    long frameBytes = headerBytes + bodyBytes;

    // Write the header info into a buffer.
    // The format is: [frame length][message type][message][(optional) data]
    ByteBuf buffer = ctx.alloc().buffer();
    buffer.writeLong(frameBytes);
    type.encode(buffer);
    in.encode(buffer);

    // Output the header buffer.
    out.add(buffer);

    if (payload != null && bodyBytes > 0) {
      Object output = payload.getNettyOutput();
      Preconditions.checkArgument(output instanceof ByteBuf || output instanceof FileRegion,
          "The payload must be a ByteBuf or a FileRegion.");
      out.add(output);
    }

  }
}
