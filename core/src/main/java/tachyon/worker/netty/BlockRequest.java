/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package tachyon.worker.netty;

import java.util.List;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Request from the client for a given block. To go from netty to this object,
 * {@link tachyon.worker.netty.BlockRequest.Decoder} is used.
 */
public final class BlockRequest {
  private final long mBlockId;
  private final long mOffset;
  private final long mLength;

  public BlockRequest(long blockId, long offset, long length) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
  }

  public long getBlockId() {
    return mBlockId;
  }

  public long getOffset() {
    return mOffset;
  }

  public long getLength() {
    return mLength;
  }

  /**
   * Creates a new {@link tachyon.worker.netty.BlockRequest} from the user's request.
   */
  public static final class Decoder extends ByteToMessageDecoder {
    private static final int MESSAGE_LENGTH = Shorts.BYTES + Longs.BYTES * 3;

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out)
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

      // remove this from the pipeline so it won't be called again for this connection
      ctx.channel().pipeline().remove(this);
    }
  }
}
