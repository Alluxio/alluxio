package tachyon.worker.netty;

import java.nio.ByteBuffer;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.FileRegion;
import tachyon.worker.DataServerMessage;

import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class BlockResponse {
  private final long BLOCK_ID;
  private final long OFFSET;
  private final long LENGTH;

  public BlockResponse(long blockId, long offset, long length) {
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

  public static final class Encoder extends MessageToByteEncoder<BlockResponse> {

    @Override
    protected void encode(final ChannelHandlerContext ctx, final BlockResponse msg,
        final ByteBuf out) throws Exception {
      out.writeShort(DataServerMessage.DATA_SERVER_RESPONSE_MESSAGE);
      out.writeLong(msg.getBlockId());
      out.writeLong(msg.getOffset());
      out.writeLong(msg.getLength());
    }
  }
}
