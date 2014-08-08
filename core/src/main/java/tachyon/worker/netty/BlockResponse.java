package tachyon.worker.netty;

import java.nio.ByteBuffer;

import tachyon.worker.DataServerMessage;

import com.google.common.base.Preconditions;

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
    DATA = Preconditions.checkNotNull(data);
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
      out.writeBytes(msg.getData());
    }
  }
}
