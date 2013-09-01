package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JavaByteBufferWriter extends ByteBufferWriter {

  public JavaByteBufferWriter(ByteBuffer buf) throws IOException {
    super(buf);
  }

  @Override
  public ByteBuffer put(Byte b) {
    return mBuf.put(b);
  }

  @Override
  public ByteBuffer put(byte[] src) {
    return mBuf.put(src);
  }

  @Override
  public ByteBuffer put(byte[] src, int offset, int length) {
    return mBuf.put(src, offset, length);
  }

  @Override
  public ByteBuffer putChar(char value) {
    return mBuf.putChar(value);
  }

  @Override
  public ByteBuffer putDouble(double value) {
    return mBuf.putDouble(value);
  }

  @Override
  public ByteBuffer putFloat(float value) {
    return mBuf.putFloat(value);
  }

  @Override
  public ByteBuffer putInt(int value) {
    return mBuf.putInt(value);
  }

  @Override
  public ByteBuffer putLong(long value) {
    return mBuf.putLong(value);
  }

  @Override
  public ByteBuffer putShort(short value) {
    return mBuf.putShort(value);
  }
}
