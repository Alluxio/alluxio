package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

final class BoundedWritableBlockChannel implements WritableBlockChannel {
  private final long maxSize;
  private final CountingWritableBlockChannel channel;

  BoundedWritableBlockChannel(final long maxSize, final WritableBlockChannel channel) {
    this.maxSize = maxSize;
    this.channel = new CountingWritableBlockChannel(channel);
  }

  @Override
  public void cancel() throws IOException {
    channel.cancel();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    if (maxSize == channel.written()) {
      // we are full
      return -1;
    } else if (src.remaining() > maxSize - channel.written()) {
      // can't write everything, so fill the channel and return what was written
      return channel.write(slice(src, (int) (maxSize - channel.written())));
    } else {
      return channel.write(src);
    }
  }

  private ByteBuffer slice(ByteBuffer src, int size) {
    byte[] data = new byte[size];
    src.get(data);
    return ByteBuffer.wrap(data);
  }

  @Override
  public boolean isOpen() {
    return channel.isOpen();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}
