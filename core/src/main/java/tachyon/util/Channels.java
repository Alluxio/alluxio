package tachyon.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public final class Channels {
  private Channels() {

  }

  public static ByteBuffer read(final ReadableByteChannel channel, int length) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    channel.read(buffer);
    buffer.flip();
    return buffer;
  }
}
