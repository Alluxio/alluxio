package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

final class BufferedWritableBlockChannel extends ForwardingWritableBlockChannel {
  private final byte[] buffer;
  private int length = 0;

  public BufferedWritableBlockChannel(int bufferSize, WritableBlockChannel delegate) {
    super(delegate);
    buffer = new byte[bufferSize];
  }


  @Override
  public int write(ByteBuffer src) throws IOException {
    if (shouldFlush(src)) {
      flush();
    }

    if (src.remaining() >= buffer.length) {
      // skip buffer since its large enough
      return delegate().write(src);
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
    delegate().write(ByteBuffer.wrap(buffer, 0, length));
    length = 0;
  }

  @Override
  public void close() throws IOException {
    if (length > 0) {
      flush();
    }
    delegate().close();
  }
}
