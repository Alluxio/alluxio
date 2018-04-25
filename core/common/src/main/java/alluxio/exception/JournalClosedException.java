/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.exception;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Exception thrown to indicate that an operation failed because the journal has been closed.
 */
@ThreadSafe
public class JournalClosedException extends Exception {
  /**
   * Constructs a <code>JournalClosedException</code> with the specified detail message.
   *
   * @param s the detail message
   */
  public JournalClosedException(String s) {
    super(s);
  }

  /**
   * @return an IOException version of this exception
   */
  public IOJournalClosedException toIOException() {
    return new IOJournalClosedException(getMessage());
  }

  /**
   * Same as {@link JournalClosedException}, but extends IOException for situations where only
   * IOException is allowed.
   */
  public static final class IOJournalClosedException extends IOException {
    /**
     * Constructs an <code>IOJournalClosedException</code> with the specified detail message.
     *
     * @param s the detail message
     */
    private IOJournalClosedException(String s) {
      super(s);
    }

    /**
     * @return a regular (non-IOException) version of this exception
     */
    public JournalClosedException toJournalClosedException() {
      return new JournalClosedException(getMessage());
    }
  }
}
