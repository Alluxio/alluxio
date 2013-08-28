package tachyon.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class ByteBufferReader {
  protected ByteBuffer mBuf;

  public ByteBufferReader(ByteBuffer buf) {
    mBuf = buf;
  }

  public abstract byte get();

  public abstract ByteBuffer get(byte[] dst);

  public abstract ByteBuffer get(byte[] dst, int offset, int length);

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
