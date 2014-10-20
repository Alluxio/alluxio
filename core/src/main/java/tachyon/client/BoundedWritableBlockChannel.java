package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

/**
 * Restricts the amount of data that a channel can have written to it. Once {@link #mMaxSize} is
 * reached, {@link #write(java.nio.ByteBuffer)} will return {@code -1}. In order to keep track of
 * how much data has been written so far, the provided channel will be wrapped around a
 * {@link tachyon.client.CountingWritableBlockChannel} which is expected to keep a accurate count of
 * how many bytes have been written.
 */
final class BoundedWritableBlockChannel extends
    ForwardingWritableBlockChannel<CountingWritableBlockChannel> {
  private final long mMaxSize;

  BoundedWritableBlockChannel(final long maxSize, final WritableBlockChannel channel) {
    super(new CountingWritableBlockChannel(channel));
    Preconditions.checkArgument(maxSize >= 0, "Max size can only be a positive number, or zero.");
    mMaxSize = maxSize;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (mMaxSize <= delegate().written()) {
      // we are full, so reject all writes
      return -1;
    } else if (src.remaining() > remaining()) {
      // can't write everything, so fill the channel and return what was written
      // since ByteBuffer currently restricts the size to a int, this operation should be safe
      return delegate().write(slice(src, (int) (mMaxSize - delegate().written())));
    } else {
      // we can consumme the message fully, pass it along the chain.
      return delegate().write(src);
    }
  }

  /**
   * Checks how many bytes are left that can be written.
   */
  private long remaining() {
    return mMaxSize - delegate().written();
  }

  /**
   * Reads {@code size} number of bytes from the given {@code src}.
   */
  private ByteBuffer slice(ByteBuffer src, int size) {
    byte[] data = new byte[size];
    src.get(data);
    return ByteBuffer.wrap(data);
  }
}
