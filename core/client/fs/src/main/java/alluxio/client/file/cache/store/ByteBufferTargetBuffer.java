package alluxio.client.file.cache.store;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteBufferTargetBuffer implements PageReadTargetBuffer {
  private final ByteBuffer mTarget;

  /**
   * Constructor
   * @param target
   */
  public ByteBufferTargetBuffer(ByteBuffer target) {
    mTarget = target;
  }

  @Override
  public boolean hasByteArray() {
    return false;
  }

  @Override
  public byte[] byteArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasByteBuffer() {
    return true;
  }

  @Override
  public ByteBuffer byteBuffer() {
    return mTarget;
  }

  @Override
  public long offset() {
    return mTarget.position();
  }

  @Override
  public WritableByteChannel byteChannel() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long remaining() {
    return mTarget.remaining();
  }
}