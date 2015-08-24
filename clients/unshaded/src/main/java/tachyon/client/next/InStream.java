package tachyon.client.next;

import java.io.IOException;
import java.io.InputStream;

public abstract class InStream extends InputStream {
  /**
   * Gets the remaining number of bytes left in the stream, starting at the current position.
   */
  public abstract long remaining();

  /**
   * Moves the starting read position of the stream to the specified position which is relative to
   * the start of the stream. Seeking to a position before the current read position is supported.
   *
   * @param pos The position to seek to, it must be between 0 and the size of the block.
   * @throws IOException if the seek fails due to an error accessing the stream at the position
   */
  public abstract void seek(long pos) throws IOException;
}
