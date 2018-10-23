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

package alluxio.exception.status;

/**
 * Exception indicating that an operation was cancelled (typically by the caller).
 */
public class CanceledException extends AlluxioStatusException {
  private static final long serialVersionUID = 7530942095354551886L;
  private static final Status STATUS = Status.CANCELED;

  /**
   * @param message the exception message
   */
  public CanceledException(String message) {
    super(STATUS, message);
  }

  /**
   * @param cause the cause of the exception
   */
  public CanceledException(Throwable cause) {
    super(STATUS, cause);
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public CanceledException(String message, Throwable cause) {
    super(STATUS, message, cause);
  }
}
