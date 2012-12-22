package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
  private ByteBuffer mByteBuffer;
  public ByteBufferInputStream(ByteBuffer buffer) {
    mByteBuffer = buffer;
  }

  @Override
  public int read() throws IOException {
    if (mByteBuffer.limit() - mByteBuffer.position() == 0) {
      return -1;
    }

    return mByteBuffer.get() & 0xFF;
  }
}