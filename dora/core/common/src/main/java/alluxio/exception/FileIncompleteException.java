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

import alluxio.AlluxioURI;

import java.text.MessageFormat;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The exception thrown when opening a file that hasn't completed.
 */
@ThreadSafe
public class FileIncompleteException extends AlluxioException {
  private static final long serialVersionUID = -4892636367696520754L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public FileIncompleteException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception stating that the given file is incomplete.
   *
   * @param path the path to the incomplete file
   */
  public FileIncompleteException(AlluxioURI path) {
    this(MessageFormat.format(
        "Cannot read from {0} because it is incomplete. Wait for the file to be marked as complete "
            + "by the writing thread or application.",
        path));
  }
}
