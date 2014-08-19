/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.netty;

import java.nio.ByteBuffer;

import tachyon.worker.DataServerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class BlockResponse {
  private final long BLOCK_ID;
  private final long OFFSET;
  private final long LENGTH;
  private final ByteBuffer DATA;

  public BlockResponse(long blockId, long offset, long length, ByteBuffer data) {
    BLOCK_ID = blockId;
    OFFSET = offset;
    LENGTH = length;
    DATA = data;
  }

  public BlockResponse(long blockId, long offset, long length) {
    this(blockId, offset, length, null);
  }

  public long getBlockId() {
    return BLOCK_ID;
  }

  public long getOffset() {
    return OFFSET;
  }

  public long getLength() {
    return LENGTH;
  }

  public ByteBuffer getData() {
    return DATA;
  }

  public static final class Encoder extends MessageToByteEncoder<BlockResponse> {

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BlockResponse msg,
        final ByteBuf out) throws Exception {
      out.writeShort(DataServerMessage.DATA_SERVER_RESPONSE_MESSAGE);
      out.writeLong(msg.getBlockId());
      out.writeLong(msg.getOffset());
      out.writeLong(msg.getLength());

      if (msg.getData() != null) {
        out.writeBytes(msg.getData());
      }
    }
  }
}
