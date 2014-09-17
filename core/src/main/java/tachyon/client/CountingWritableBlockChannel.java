package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keeps track of how many bytes get written to the delegating channel.
 */
final class CountingWritableBlockChannel extends ForwardingWritableBlockChannel {
  private final AtomicLong mWritten = new AtomicLong(0);

  CountingWritableBlockChannel(WritableBlockChannel delegate) {
    super(delegate);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int r = delegate().write(src);
    // if r is -1, then don't add to written count!
    if (r > 0) {
      mWritten.addAndGet(r);
    }
    return r;
  }

  /**
   * Returns how much data has been written so far.
   */
  public long written() {
    return mWritten.get();
  }
}
