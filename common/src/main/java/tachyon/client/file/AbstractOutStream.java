package tachyon.client.file;

import java.io.OutputStream;

/**
 * An abstraction of the output stream API in Tachyon to write data to a file or a block. In
 * addition to extending abstract class {@link OutputStream} as the basics, it also keeps
 * counting the number of bytes written to the output stream.
 */
public abstract class AbstractOutStream extends OutputStream {
  // TODO(binfan): make count long.
  /** The number of bytes written */
  protected int mCount = 0;

  /**
   * @return the number of bytes written to this stream
   */
  public int getCount() {
    return mCount;
  }
}
