package tachyon.io;

import java.nio.ByteBuffer;

public class JavaByteBufferReader extends ByteBufferReader {

  public JavaByteBufferReader(ByteBuffer buf) {
    super(buf);
  }

  @Override
  public byte get() {
    return mBuf.get();
  }

  @Override
  public ByteBuffer get(byte[] dst) {
    return mBuf.get(dst);
  }

  @Override
  public ByteBuffer get(byte[] dst, int offset, int length) {
    return mBuf.get(dst, offset, length);
  }

  @Override
  public char getChar() {
    return mBuf.getChar();
  }

  @Override
  public double getDouble() {
    return mBuf.getDouble();
  }

  @Override
  public float getFloat() {
    return mBuf.getFloat();
  }

  @Override
  public int getInt() {
    return mBuf.getInt();
  }

  @Override
  public long getLong() {
    return mBuf.getLong();
  }

  @Override
  public short getShort() {
    return mBuf.getShort();
  }

  @Override
  public int position() {
    return mBuf.position();
  }

  @Override
  public void position(int newPosition) {
    mBuf.position(newPosition);
  }
}
