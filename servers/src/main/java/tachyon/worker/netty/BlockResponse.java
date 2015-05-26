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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockHandler;
import tachyon.worker.DataServerMessage;

/**
 * The response to a user-sent {@link tachyon.worker.netty.protocol.RPCBlockRequest}.
 * <p>
 * Response is serialized before sent to network by
 * {@link tachyon.worker.netty.BlockResponse.Encoder}
 * </p>
 */
public final class BlockResponse {
  /**
   * Encodes a {@link tachyon.worker.netty.BlockResponse} to network.
   */
  public static final class Encoder extends MessageToMessageEncoder<BlockResponse> {
    // TODO: remove hardcoded header lengths.
    private static final int MESSAGE_LENGTH = Shorts.BYTES + Longs.BYTES * 3 + Longs.BYTES
        + Ints.BYTES;
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    private final TachyonConf mTachyonConf;

    public Encoder(TachyonConf tachyonConf) {
      super();

      mTachyonConf = tachyonConf;
    }

    private ByteBuf createHeader(final ChannelHandlerContext ctx, final BlockResponse msg) {
      ByteBuf header = ctx.alloc().buffer(MESSAGE_LENGTH);
      // These two fields are hard coded for now.
      header.writeLong(38 + msg.getLength()); // frame length
      header.writeInt(0); // RPC message type
      header.writeShort(DataServerMessage.DATA_SERVER_RESPONSE_MESSAGE);
      header.writeLong(msg.getBlockId());
      header.writeLong(msg.getOffset());
      header.writeLong(msg.getLength());
      return header;
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BlockResponse msg,
        final List<Object> out) throws Exception {

      // Add the header information to output
      out.add(createHeader(ctx, msg));

      final BlockHandler handler = msg.getHandler();
      if (handler == null) {
        return;
      }
      final FileTransferType type =
          mTachyonConf
              .getEnum(Constants.WORKER_NETTY_FILE_TRANSFER_TYPE, FileTransferType.TRANSFER);
      switch (type) {
        case MAPPED:
          ByteBuffer data = handler.read(msg.getOffset(), (int) msg.getLength());
          out.add(Unpooled.wrappedBuffer(data));
          handler.close();
          break;
        case TRANSFER: // intend to fall through as TRANSFER is the default type.
        default:
          if (handler.getChannel() instanceof FileChannel) {
            out.add(new DefaultFileRegion((FileChannel) handler.getChannel(), msg.getOffset(), msg
                .getLength()));
          } else {
            handler.close();
            throw new Exception("Only FileChannel is supported!");
          }
          break;
      }
    }
  }

  /**
   * Creates a {@link tachyon.worker.netty.BlockResponse} that indicates an error for the given
   * block.
   *
   * @param blockId The Id of block requested
   * @return the new error BlockResponse created.
   */
  public static BlockResponse createErrorResponse(final long blockId) {
    return new BlockResponse(-blockId, 0, 0, null);
  }

  private final long mBlockId;
  private final long mOffset;
  private final long mLength;
  private final BlockHandler mHandler;

  public BlockResponse(long blockId, long offset, long length, BlockHandler handler) {
    mBlockId = blockId;
    mOffset = offset;
    mLength = length;
    mHandler = handler;
  }

  public long getBlockId() {
    return mBlockId;
  }

  public BlockHandler getHandler() {
    return mHandler;
  }

  public long getLength() {
    return mLength;
  }

  public long getOffset() {
    return mOffset;
  }
}
