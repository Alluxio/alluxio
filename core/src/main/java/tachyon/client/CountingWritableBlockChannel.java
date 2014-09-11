package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public final class CountingWritableBlockChannel implements WritableBlockChannel {
  private final AtomicLong mWritten = new AtomicLong(0);
  private final WritableBlockChannel mDelegate;

  public CountingWritableBlockChannel(WritableBlockChannel delegate) {
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

  @Override
  public void cancel() throws IOException {
    mDelegate.cancel();
  }

  public long written() {
    return mWritten.get();
  }
}
