package tachyon.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

public final class CountingWritableByteChannel implements WritableByteChannel {
  private final AtomicLong mWritten = new AtomicLong(0);
  private final WritableByteChannel mDelegate;

  public CountingWritableByteChannel(WritableByteChannel delegate) {
    this.mDelegate = delegate;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int r = mDelegate.write(src);
    mWritten.addAndGet(r);
    return r;
  }

  @Override
  public boolean isOpen() {
    return mDelegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    mDelegate.close();
  }

  public long written() {
    return mWritten.get();
  }
}
