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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Request from the client for a given block. To go from netty to this object,
 * {@link tachyon.worker.netty.BlockRequest.Decoder} is used.
 */
public final class BlockRequest {
  private final long BLOCK_ID;
  private final long OFFSET;
  private final long LENGTH;

  public BlockRequest(long blockId, long offset, long length) {
    BLOCK_ID = blockId;
    OFFSET = offset;
    LENGTH = length;
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

  /**
   * Creates a new {@link tachyon.worker.netty.BlockRequest} from the user's request.
   */
  public static final class Decoder extends ByteToMessageDecoder {
    private static final int LONG_SIZE = 8;
    private static final int SHORT_SIZE = 2;
    private static final int MESSAGE_LENGTH = SHORT_SIZE + LONG_SIZE * 3;

    @Override
    protected void
        decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
            throws Exception {
      if (in.readableBytes() < MESSAGE_LENGTH) {
        return;
      }

      // read the type and ignore it. Currently only one type exists
      in.readShort(); // == DataServerMessage.DATA_SERVER_REQUEST_MESSAGE;
      long blockId = in.readLong();
      long offset = in.readLong();
      long length = in.readLong();

      out.add(new BlockRequest(blockId, offset, length));
    }
  }
}
