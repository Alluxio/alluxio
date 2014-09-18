package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class BufferedWritableBlockChannel extends ForwardingWritableBlockChannel {
  private final ByteBuffer buffer;

  public BufferedWritableBlockChannel(int bufferSize, WritableBlockChannel delegate) {
    super(delegate);
    buffer = ByteBuffer.allocate(bufferSize);
  }


  @Override
  public int write(ByteBuffer src) throws IOException {
    if (shouldFlush(src)) {
      flush();
    }

    if (src.remaining() >= buffer.capacity()) {
      // skip buffer since its large enough
      return delegate().write(src);
    } else {
      // write to local buffer and return
      int r = src.remaining();
      buffer.put(src);
      return r;
    }
  }

  private boolean shouldFlush(ByteBuffer src) {
    return buffer.position() + src.remaining() > buffer.limit();
  }

  private void flush() throws IOException {
    if (buffer.position() != 0) {
      // empty, so ignore
      buffer.flip();
      delegate().write(buffer);
      buffer.clear();
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    delegate().close();
  }
}
