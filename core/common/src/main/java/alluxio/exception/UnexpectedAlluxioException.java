/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.exception;

/**
 * The exception thrown when an unexpected error occurs within the Alluxio system.
 */
public final class UnexpectedAlluxioException extends AlluxioException {
  private static final long serialVersionUID = -1029072354884843903L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public UnexpectedAlluxioException(String message) {
    super(message);
  }

  /**
   * @param e an exception to wrap
   */
  public UnexpectedAlluxioException(RuntimeException e) {
    super(e);
  }
}
