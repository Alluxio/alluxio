package tachyon.worker.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

/**
 * Request from the client for a given block. To go from netty to this object,
 * {@link tachyon.worker.netty.BlockRequest.Decoder} is used.
 */
public final class BlockRequest {
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

  public long getLength() {
    return mLength;
  }

  public long getOffset() {
    return mOffset;
  }
}
