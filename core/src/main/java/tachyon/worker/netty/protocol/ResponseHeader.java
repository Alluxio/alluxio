package tachyon.worker.netty.protocol;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBufAllocator;
import tachyon.worker.netty.NettyWritable;

import java.io.IOException;
import java.util.List;

public final class ResponseHeader implements NettyWritable {
  public static final int HEADER_SIZE = Ints.BYTES;

  private final ResponseType type;

  public ResponseHeader(ResponseType type) {
    this.type = type;
  }

  public ResponseType getType() {
    return type;
  }

  @Override
  public List<Object> write(ByteBufAllocator allocator) throws IOException {
    return ImmutableList.<Object>of(
        allocator.buffer(HEADER_SIZE).writeInt(type.ordinal())
    );
  }
}
