package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The wrapper for to read ByteBuffer using ByteBuffer's default methods.
 */
public class JavaByteBufferReader extends ByteBufferReader {
  public JavaByteBufferReader(ByteBuffer buf) throws IOException {
    super(buf);
  }

  @Override
  public byte get() {
    return mBuf.get();
  }

  @Override
  public void get(byte[] dst) {
    mBuf.get(dst);
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    mBuf.get(dst, offset, length);
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
