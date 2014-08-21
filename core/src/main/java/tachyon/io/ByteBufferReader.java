package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reader for bytebuffer.
 */
public abstract class ByteBufferReader {
  /**
   * Get most efficient ByteBufferReader for the ByteBuffer.
   * 
   * @param buf
   *          the ByteBuffer to read.
   * @return The most efficient ByteBufferReader for buf.
   * @throws IOException
   */
  public static ByteBufferReader getByteBufferReader(ByteBuffer buf) throws IOException {
    // if (buf.order() == ByteOrder.nativeOrder()) {
    // if (buf.isDirect()) {
    // return new UnsafeDirectByteBufferReader(buf);
    // } else {
    // return new UnsafeHeapByteBufferReader(buf);
    // }
    // }
    return new JavaByteBufferReader(buf);
  }

  protected ByteBuffer mBuf;

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

  public ByteOrder order() {
    return mBuf.order();
  }

  public void order(ByteOrder bo) {
    mBuf.order(bo);
  }

  public abstract int position();

  public abstract void position(int newPosition);
}
