package alluxio.client.file.cache.store;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteArrayTargetBuffer implements PageReadTargetBuffer {
  private final byte[] mTarget;
  private int mOffset;

  /**
   * Constructor.
   * @param target
   * @param offset
   */
  public ByteArrayTargetBuffer(byte[] target, int offset) {
    mTarget = target;
    mOffset = offset;
  }

  @Override
  public boolean hasByteArray() {
    return true;
  }

  @Override
  public byte[] byteArray() {
    return mTarget;
  }

  @Override
  public boolean hasByteBuffer() {
    return false;
  }

  @Override
  public ByteBuffer byteBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long offset() {
    return mOffset;
  }

  @Override
  public long remaining() {
    return mTarget.length - mOffset;
  }

  @Override
  public WritableByteChannel byteChannel() {
    throw new UnsupportedOperationException();
  }
}
