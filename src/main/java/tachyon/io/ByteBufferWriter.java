package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class ByteBufferWriter {
  protected ByteBuffer mBuf;

  public ByteBufferWriter(ByteBuffer buf) throws IOException {
    if (buf == null) {
      throw new IOException("ByteBuffer is null");
    }

    mBuf = buf;
  }

  public abstract ByteBuffer put(Byte b);

  public abstract ByteBuffer put(byte[] src);

  public abstract ByteBuffer put(byte[] src, int offset, int length);

  public abstract ByteBuffer putChar(char value);

  public abstract ByteBuffer putDouble(double value);

  public abstract ByteBuffer putFloat(float value);

  public abstract ByteBuffer putInt(int value);

  public abstract ByteBuffer putLong(long value);

  public abstract ByteBuffer putShort(short value);

  public ByteOrder order() {
    return mBuf.order();
  }

  public ByteBuffer order(ByteOrder bo) {
    return mBuf.order(bo);
  }

  public ByteBuffer generateByteBuffer() {
    mBuf.flip();
    return mBuf;
  }
}
