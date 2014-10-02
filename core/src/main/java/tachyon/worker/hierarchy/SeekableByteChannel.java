package tachyon.worker.hierarchy;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Fork of java 7's SeekableByteChannel.
 */
public interface SeekableByteChannel extends ReadableByteChannel, WritableByteChannel {
  long position();

  SeekableByteChannel position(long newPosition);

  long	transferTo(long position, long count, WritableByteChannel target) throws IOException;
}
