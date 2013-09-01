package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class ByteBufferReader {
  protected ByteBuffer mBuf;

  /**
   * Get most efficient ByteBufferReader for the ByteBuffer.
   * @param buf the ByteBuffer to read.
   * @return The most efficient ByteBufferReader for buf.
   * @throws IOException 
   */
  public static ByteBufferReader getByteBufferReader(ByteBuffer buf) throws IOException {
    if (buf.order() == ByteOrder.nativeOrder()) {
      if (buf.isDirect()) {
        return new UnsafeDirectByteBufferReader(buf);
      } else {
        return new UnsafeHeapByteBufferReader(buf);
      }
    }
    return new JavaByteBufferReader(buf);
  }

  ByteBufferReader(ByteBuffer buf) throws IOException {
    if (buf == null) {
      throw new IOException("ByteBuffer is null");
    }

    mBuf = buf;
  }

  public abstract byte get();

  public abstract void get(byte[] dst);

  public abstract void get(byte[] dst, int offset, int length);

  public abstract char getChar();

  public abstract double getDouble();

  public abstract float getFloat();

  public abstract int getInt();

  public abstract long getLong();

  public abstract short getShort();

  public abstract int position();

  public abstract void position(int newPosition);

  public ByteOrder order() {
    return mBuf.order();
  }

  public void order(ByteOrder bo) {
    mBuf.order(bo);
  }
}
