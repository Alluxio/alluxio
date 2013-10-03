package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The wrapper for to write ByteBuffer using ByteBuffer's default methods.
 */
public class JavaByteBufferWriter extends ByteBufferWriter {

  public JavaByteBufferWriter(ByteBuffer buf) throws IOException {
    super(buf);
  }

  @Override
  public void put(Byte b) {
    mBuf.put(b);
  }

  @Override
  public void put(byte[] src, int offset, int length) {
    mBuf.put(src, offset, length);
  }

  @Override
  public void putChar(char value) {
    mBuf.putChar(value);
  }

  @Override
  public void putDouble(double value) {
    mBuf.putDouble(value);
  }

  @Override
  public void putFloat(float value) {
    mBuf.putFloat(value);
  }

  @Override
  public void putInt(int value) {
    mBuf.putInt(value);
  }

  @Override
  public void putLong(long value) {
    mBuf.putLong(value);
  }

  @Override
  public void putShort(short value) {
    mBuf.putShort(value);
  }

  @Override
  public ByteBuffer getByteBuffer() {
    ByteBuffer buf = mBuf.duplicate();
    buf.position(0);
    buf.limit(mBuf.position());
    buf.order(mBuf.order());
    return buf;
  }
}
