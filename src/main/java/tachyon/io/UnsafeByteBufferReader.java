package tachyon.io;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;
import tachyon.CommonUtils;

/**
 * The buffer must be direct bytebuffer.
 */
public class UnsafeByteBufferReader extends ByteBufferReader {
  private Unsafe UNSAFE;
  private long mBaseOffset;
  private long mOffset;

  public UnsafeByteBufferReader(ByteBuffer buf) {
    super(buf);

    Field theUnsafe;
    try {
      theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);

      Field addressField = Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
      mBaseOffset = (long) addressField.get(buf);
      mOffset = mBaseOffset;
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | 
        IllegalAccessException e) {
      CommonUtils.runtimeException(e);
    }
  }

  @Override
  public byte get() {
    return Unsafe.getUnsafe().getByte(mOffset ++);
  }

  @Override
  public ByteBuffer get(byte[] dst) {
    

    Unsafe.getUnsafe().copyMemory(null, mOffset, dst, Unsafe.getUnsafe(), arg4);
    .unsafe.copyMemory(null, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length)
    _offset += length
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ByteBuffer get(byte[] dst, int offset, int length) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public char getChar() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double getDouble() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getFloat() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getInt() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getLong() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public short getShort() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int position() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void position(int newPosition) {
    // TODO Auto-generated method stub

  }
}
