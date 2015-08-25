package tachyon.client.next;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Provides a stream API to write to Tachyon. Only one OutStream should be opened for a Tachyon
 * object. This class is not thread safe and should only be used by one thread.
 */
public abstract class OutStream extends OutputStream {
  /**
   * Cancels the write to Tachyon storage. This will delete all the temporary data and metadata
   * that has been written to the worker(s). This method should be called when a write is aborted.
   *
   * @throws IOException if there is a failure when the worker invalidates the cache attempt
   */
  public abstract void cancel() throws IOException;
}
