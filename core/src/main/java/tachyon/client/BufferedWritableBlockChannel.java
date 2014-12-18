package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Buffers {@link #write(java.nio.ByteBuffer)} requests until {@code bufferSize} is reached. When
 * {@link #close()} is called, the remaining buffer will be flushed to the underline channel.
 */
final class BufferedWritableBlockChannel extends ForwardingWritableBlockChannel {
  private final ByteBuffer mBuffer;

  public BufferedWritableBlockChannel(int bufferSize, WritableBlockChannel delegate) {
    super(delegate);
    mBuffer = ByteBuffer.allocate(bufferSize);
  }


  @Override
  public int write(ByteBuffer src) throws IOException {
    if (shouldFlush(src)) {
      flush();
    }

    if (src.remaining() >= mBuffer.capacity()) {
      // skip buffer since its large enough
      return delegate().write(src);
    } else {
      // write to local buffer and return
      int r = src.remaining();
      mBuffer.put(src);
      return r;
    }
  }

  private boolean shouldFlush(ByteBuffer src) {
    return mBuffer.position() + src.remaining() > mBuffer.limit();
  }

  private void flush() throws IOException {
    if (mBuffer.position() != 0) {
      // empty, so ignore
      mBuffer.flip();
      delegate().write(mBuffer);
      mBuffer.clear();
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    delegate().close();
  }
}
