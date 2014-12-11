package tachyon.worker.netty.protocol;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public abstract class StringError extends Error {
  private final String mMessage;

  public StringError(ResponseType mType, String message) {
    super(mType);
    this.mMessage = message;
  }

  public String getmMessage() {
    return mMessage;
  }

  @Override
  public List<Object> write(ByteBufAllocator allocator) throws IOException {
    ImmutableList.Builder<Object> builder = new ImmutableList.Builder<Object>();
    builder.addAll(super.write(allocator));

    byte[] data = mMessage.getBytes(Charsets.UTF_8);
    ByteBuf buffer = allocator.buffer(Ints.BYTES + data.length);
    buffer.writeInt(data.length);
    buffer.writeBytes(data);

    builder.add(buffer);
    return builder.build();
  }
}
