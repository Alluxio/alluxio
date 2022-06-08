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
 * The exception thrown when trying to call openFile on a directory.
 */
@ThreadSafe
public class OpenDirectoryException extends AlluxioException {
  private static final long serialVersionUID = 1101354870166829139L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public OpenDirectoryException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception stating that the given directory cannot be opened for reading.
   *
   * @param path the path to the directory
   */
  public OpenDirectoryException(AlluxioURI path) {
    this(MessageFormat.format("Cannot read from {0} because it is a directory", path));
  }
}
