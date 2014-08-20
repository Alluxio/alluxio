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
import java.nio.channels.FileChannel;
import java.util.List;

import tachyon.conf.WorkerConf;
import tachyon.worker.nio.DataServerMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * When a user sends a {@link tachyon.worker.netty.BlockRequest}, the response back is of this type.
 * <p />
 * To serialize the response to network, {@link tachyon.worker.netty.BlockResponse.Encoder} is used.
 */
public final class BlockResponse {
  private final long BLOCK_ID;
  private final long OFFSET;
  private final long LENGTH;
  private final FileChannel CHANNEL;

  public BlockResponse(long blockId, long offset, long length, FileChannel channel) {
    BLOCK_ID = blockId;
    OFFSET = offset;
    LENGTH = length;
    CHANNEL = channel;
  }

  /**
   * Creates a {@link tachyon.worker.netty.BlockResponse} that represents a error case for the
   * given block.
   */
  public static BlockResponse createErrorResponse(final long blockId) {
    return new BlockResponse(-blockId, 0, 0, null);
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

  public FileChannel getChannel() {
    return CHANNEL;
  }

  /**
   * Encodes a {@link tachyon.worker.netty.BlockResponse} to network.
   */
  public static final class Encoder extends MessageToMessageEncoder<BlockResponse> {
    private static final int LONG_SIZE = 8;
    private static final int SHORT_SIZE = 2;
    private static final int MESSAGE_LENGTH = SHORT_SIZE + LONG_SIZE * 3;

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BlockResponse msg,
        final List<Object> out) throws Exception {
      out.add(createHeader(ctx, msg));
      if (msg.getChannel() != null) {
        switch (WorkerConf.get().NETTY_FILE_STREAM_TYPE) {
        case TRANSFER:
          out.add(new DefaultFileRegion(msg.getChannel(), msg.getOffset(), msg.getLength()));
          break;
        case MAPPED:
          ByteBuffer data =
              msg.getChannel()
                  .map(FileChannel.MapMode.READ_ONLY, msg.getOffset(), msg.getLength());
          out.add(Unpooled.wrappedBuffer(data));
          break;
        default:
          throw new AssertionError("Unsupported stream type: " +
              WorkerConf.get().NETTY_FILE_STREAM_TYPE);
        }
      }
    }

    private ByteBuf createHeader(final ChannelHandlerContext ctx, final BlockResponse msg) {
      ByteBuf header = ctx.alloc().buffer(MESSAGE_LENGTH);
      header.writeShort(DataServerMessage.DATA_SERVER_RESPONSE_MESSAGE);
      header.writeLong(msg.getBlockId());
      header.writeLong(msg.getOffset());
      header.writeLong(msg.getLength());
      return header;
    }
  }
}
