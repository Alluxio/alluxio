package tachyon.worker.netty.protocol;

import java.util.List;

import com.google.common.primitives.Longs;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public final class GetBlock {
  public static final long REQUEST_SIZE = 3 * Longs.BYTES;

  private final long mBlockId;
  private final long mOffset;
  private final long mLength;

  public GetBlock(long blockId, long offset, long length) {
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

  public boolean hasLength() {
    return mLength > 0;
  }

  public long getLength() {
    return mLength;
  }

  public static final class GetBlockDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        throws Exception {
      if (in.readableBytes() >= REQUEST_SIZE) {
        // offset defaults to zero
        GetBlock get = new GetBlock(in.readLong(), or(in.readLong(), 0), in.readLong());
        if (get.getBlockId() < 0) {
          Error.writeAndClose(new InvalidBlockId(get.getBlockId()), ctx);
        } else if (get.getOffset() < -1 || get.getLength() < -1) {
          // -1 is used to indicate ignore
          Error.writeAndClose(
              new InvalidBlockRange(get.getBlockId(), get.getOffset(), get.getLength()), ctx);
        } else {
          out.add(get);
          ctx.pipeline().remove(this);
        }
      }
    }

    private static long or(long value, long defaultValue) {
      if (value == -1) {
        return defaultValue;
      } else {
        return value;
      }
    }
  }
}
