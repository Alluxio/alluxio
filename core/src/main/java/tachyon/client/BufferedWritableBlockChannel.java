package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class BufferedWritableBlockChannel implements WritableBlockChannel {
  private final byte[] buffer;
  private int length = 0;
  private final WritableBlockChannel delegate;

  public BufferedWritableBlockChannel(int bufferSize, WritableBlockChannel delegate) {
    this.delegate = delegate;
    buffer = new byte[bufferSize];
  }

  @Override
  public void cancel() throws IOException {
    delegate.cancel();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (shouldFlush(src)) {
      flush();
    }

    if (src.remaining() >= buffer.length) {
      // skip buffer since its large enough
      return delegate.write(src);
    } else {
      // write to local buffer and return
      int r = src.remaining();
      src.get(buffer, length, r);
      length += r;
      return r;
    }
  }

  private boolean shouldFlush(ByteBuffer src) {
    return length + src.remaining() > buffer.length;
  }

  private void flush() throws IOException {
    delegate.write(ByteBuffer.wrap(buffer, 0, length));
    length = 0;
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (length > 0) {
      flush();
    }
    delegate.close();
  }
}
