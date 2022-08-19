package alluxio.worker.page;

import alluxio.client.file.cache.store.PageReadTargetBuffer;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 *
 */
public class PagedTargetByteBuf implements PageReadTargetBuffer {
  private final ByteBuf mTarget;
  private final long mLength;
  private long mOffset = 0;

  public PagedTargetByteBuf(ByteBuf target, long length) {
    mTarget = target;
    mLength = length;
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
  public WritableByteChannel byteChannel() {
    return new WritableByteChannel() {
      @Override
      public int write(ByteBuffer src) throws IOException {
        int readableBytes = src.remaining();
        mTarget.writeBytes(src);
        return readableBytes - src.remaining();
      }

      @Override
      public boolean isOpen() {
        return true;
      }

      @Override
      public void close() throws IOException {
      }
    };
  }

  @Override
  public long remaining() {
    return mTarget.writableBytes();
  }
}
