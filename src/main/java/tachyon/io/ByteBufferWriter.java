package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Writer for bytebuffer.
 */
public abstract class ByteBufferWriter {
  protected ByteBuffer mBuf;

  /**
   * Get most efficient ByteBufferWriter for the ByteBuffer.
   * @param buf the ByteBuffer to write.
   * @return The most efficient ByteBufferWriter for buf.
   * @throws IOException 
   */
  public static ByteBufferWriter getByteBufferWriter(ByteBuffer buf) throws IOException {
    if (buf.order() == ByteOrder.nativeOrder()) {
      if (buf.isDirect()) {
        return new UnsafeDirectByteBufferWriter(buf);
      } else {
        return new UnsafeHeapByteBufferWriter(buf);
      }
    }
    return new JavaByteBufferWriter(buf);
  }

  ByteBufferWriter(ByteBuffer buf) throws IOException {
    if (buf == null) {
      throw new IOException("ByteBuffer is null");
    }

    mBuf = buf;
  }

  public abstract void put(Byte b);

  public abstract void put(byte[] src);

  public abstract void put(byte[] src, int offset, int length);

  public abstract void putChar(char value);

  public abstract void putDouble(double value);

  public abstract void putFloat(float value);

  public abstract void putInt(int value);

  public abstract void putLong(long value);

  public abstract void putShort(short value);

  public ByteOrder order() {
    return mBuf.order();
  }

  public abstract ByteBuffer getByteBuffer();
}
