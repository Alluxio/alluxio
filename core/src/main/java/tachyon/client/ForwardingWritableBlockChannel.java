package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class ForwardingWritableBlockChannel<T extends WritableBlockChannel> implements
    WritableBlockChannel {
  private final T mDelegate;

  ForwardingWritableBlockChannel(T delegate) {
    this.mDelegate = delegate;
  }

  @Override
  public void cancel() throws IOException {
    delegate().cancel();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return delegate().write(src);
  }

  @Override
  public boolean isOpen() {
    return delegate().isOpen();
  }

  @Override
  public void close() throws IOException {
    delegate().close();
  }

  public T delegate() {
    return mDelegate;
  }
}
