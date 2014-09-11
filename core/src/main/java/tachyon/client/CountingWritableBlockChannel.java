package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

final class CountingWritableBlockChannel extends ForwardingWritableBlockChannel {
  private final AtomicLong mWritten = new AtomicLong(0);

  public CountingWritableBlockChannel(WritableBlockChannel delegate) {
    super(delegate);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int r = delegate().write(src);
    if (r > 0) {
      mWritten.addAndGet(r);
    }
    return r;
  }

  public long written() {
    return mWritten.get();
  }
}
