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
