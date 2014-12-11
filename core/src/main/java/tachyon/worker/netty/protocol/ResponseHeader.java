package tachyon.worker.netty.protocol;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBufAllocator;

import tachyon.worker.netty.NettyWritable;

public final class ResponseHeader implements NettyWritable {
  public static final int HEADER_SIZE = Ints.BYTES;

  private final ResponseType mType;

  public ResponseHeader(ResponseType type) {
    mType = type;
  }

  public ResponseType getType() {
    return mType;
  }

  @Override
  public List<Object> write(ByteBufAllocator allocator) throws IOException {
    return ImmutableList.<Object>of(allocator.buffer(HEADER_SIZE).writeInt(mType.ordinal()));
  }
}
