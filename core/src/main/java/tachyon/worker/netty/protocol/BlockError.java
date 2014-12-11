package tachyon.worker.netty.protocol;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;

import io.netty.buffer.ByteBufAllocator;

public abstract class BlockError extends Error {
  private final long mBlockId;

  public BlockError(ResponseType mType, long blockId) {
    super(mType);
    mBlockId = blockId;
  }

  public long getBlockId() {
    return mBlockId;
  }

  @Override
  public List<Object> write(ByteBufAllocator allocator) throws IOException {
    ImmutableList.Builder<Object> builder = new ImmutableList.Builder<Object>();
    builder.addAll(super.write(allocator));
    builder.add(allocator.buffer(Longs.BYTES).writeLong(mBlockId));
    return builder.build();
  }
}
