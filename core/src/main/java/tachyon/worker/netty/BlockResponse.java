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

package tachyon.worker.netty;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import tachyon.conf.WorkerConf;
import tachyon.worker.netty.protocol.ResponseType;

/**
 * When a user sends a {@link tachyon.worker.netty.BlockRequest}, the response back is of this type.
 * <p />
 * To serialize the response to network, {@link tachyon.worker.netty.BlockResponse.Encoder} is used.
 */
public final class BlockResponse {
  /**
   * Encodes a {@link tachyon.worker.netty.BlockResponse} to network.
   */
  public static final class Encoder extends MessageToMessageEncoder<BlockResponse> {
    private static final int MESSAGE_LENGTH = Shorts.BYTES + Longs.BYTES * 3;

    private ByteBuf createHeader(final ChannelHandlerContext ctx, final BlockResponse msg) {
      ByteBuf header = ctx.alloc().buffer(MESSAGE_LENGTH);
      header.writeShort(ResponseType.GetBlockResponse.ordinal());
      header.writeLong(msg.getBlockId());
      header.writeLong(msg.getOffset());
      header.writeLong(msg.getLength());
      return header;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BlockResponse msg,
        final List<Object> out) throws Exception {
      out.add(createHeader(ctx, msg));
      if (msg.getChannel() != null) {
        switch (WorkerConf.get().NETTY_FILE_TRANSFER_TYPE) {
          case MAPPED:
            MappedByteBuffer data =
                msg.getChannel().map(FileChannel.MapMode.READ_ONLY, msg.getOffset(),
                    msg.getLength());
            out.add(Unpooled.wrappedBuffer(data));
            msg.getChannel().close();
            break;
          case TRANSFER:
            out.add(new DefaultFileRegion(msg.getChannel(), msg.getOffset(), msg.getLength()));
            break;
          default:
            throw new AssertionError("Unknown file transfer type: "
                + WorkerConf.get().NETTY_FILE_TRANSFER_TYPE);
        }
      }
    }
  }

  /**
   * Creates a {@link tachyon.worker.netty.BlockResponse} that represents a error case for the given
   * block.
   */
  public static BlockResponse createErrorResponse(final long blockId) {
    return new BlockResponse(-blockId, 0, 0, null);
  }

  private final long mBlockId;
  private final long mOffset;

  private final long mLength;

  private final FileChannel mChannel;

  public BlockResponse(long blockId, long offset, long length, FileChannel channel) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mChannel = channel;
  }

  public long getBlockId() {
    return mBlockId;
  }

  public FileChannel getChannel() {
    return mChannel;
  }

  public long getLength() {
    return mLength;
  }

  public long getOffset() {
    return mOffset;
  }
}
