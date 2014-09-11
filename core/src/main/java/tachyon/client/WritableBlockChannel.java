package tachyon.client;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

interface WritableBlockChannel extends WritableByteChannel {

  /**
   * Similar to {@link #close()}, but indicates that any permanent resources should be freed.
   * 
   * @throws IOException
   */
  void cancel() throws IOException;
}
