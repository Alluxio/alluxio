package tachyon.io;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

/**
 * Unsafe writer for direct bytebuffer.
 */
public class UnsafeDirectByteBufferWriter extends ByteBufferWriter {
  private Unsafe mUnsafe;
  private long mBaseOffset;
  private long mOffset;

  public UnsafeDirectByteBufferWriter(ByteBuffer buf) throws IOException {
    super(buf);

    if (!buf.isDirect()) {
      throw new IOException("ByteBuffer " + buf + " is not Direct ByteBuffer");
    }
    if (buf.order() != ByteOrder.nativeOrder()) {
      throw new IOException("ByteBuffer " + buf + " has non-native ByteOrder");
    }

    try {
      mUnsafe = UnsafeUtils.getUnsafe();

      Field addressField = Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
      mBaseOffset = (long) addressField.get(buf);
      mOffset = mBaseOffset;
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | 
        IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(Byte b) {
    mUnsafe.putByte(mOffset ++,  b);
  }

  @Override
  public void put(byte[] src, int offset, int length) {
    mUnsafe.copyMemory(src, UnsafeUtils.sByteArrayBaseOffset + offset, null, mOffset, length);
    mOffset += length;
  }

  @Override
  public void putChar(char value) {
    mUnsafe.putChar(mOffset, value);
    mOffset += 2;
  }

  @Override
  public void putDouble(double value) {
    mUnsafe.putDouble(mOffset, value);
    mOffset += 8;
  }

  @Override
  public void putFloat(float value) {
    mUnsafe.putFloat(mOffset, value);
    mOffset += 4;
  }

  @Override
  public void putInt(int value) {
    mUnsafe.putInt(mOffset, value);
    mOffset += 4;
  }

  @Override
  public void putLong(long value) {
    mUnsafe.putLong(mOffset, value);
    mOffset += 8;
  }

  @Override
  public void putShort(short value) {
    mUnsafe.putShort(mOffset, value);
    mOffset += 2;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    ByteBuffer buf = mBuf.duplicate();
    buf.position(0);
    buf.limit((int) (mOffset - mBaseOffset));
    buf.order(mBuf.order());
    return buf;
  }
}
