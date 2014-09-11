package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

final class BoundedWritableBlockChannel extends
    ForwardingWritableBlockChannel<CountingWritableBlockChannel> {
  private final long maxSize;

  BoundedWritableBlockChannel(final long maxSize, final WritableBlockChannel channel) {
    super(new CountingWritableBlockChannel(channel));
    this.maxSize = maxSize;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (maxSize == delegate().written()) {
      // we are full
      return -1;
    } else if (src.remaining() > maxSize - delegate().written()) {
      // can't write everything, so fill the channel and return what was written
      return delegate().write(slice(src, (int) (maxSize - delegate().written())));
    } else {
      return delegate().write(src);
    }
  }

  private ByteBuffer slice(ByteBuffer src, int size) {
    byte[] data = new byte[size];
    src.get(data);
    return ByteBuffer.wrap(data);
  }
}
