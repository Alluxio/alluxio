package tachyon.worker.netty;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

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

  public static class Decoder extends ByteToMessageDecoder {
    private static final int LONG_SIZE = 8;
    private static final int MESSAGE_LENGTH = LONG_SIZE * 3 + 2;

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
