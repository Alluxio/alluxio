package alluxio.exception;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Exception thrown to indicate that an operation failed because the journal has been closed.
 */
@ThreadSafe
public class JournalClosedException extends Exception {
  /**
   * Constructs a <code>JournalClosedException</code> with no detail message.
   */
  public JournalClosedException() {
    super();
  }

  /**
   * Constructs a <code>JournalClosedException</code> with the specified detail message.
   *
   * @param s the detail message
   */
  public JournalClosedException(String s) {
    super(s);
  }
}
