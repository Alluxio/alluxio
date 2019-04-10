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

import io.grpc.Status;

/**
 * Exception indicating that an operation was cancelled (typically by the caller).
 */
public class CancelledException extends AlluxioStatusException {
  private static final long serialVersionUID = 7530942095354551886L;
  private static final Status STATUS = Status.CANCELLED;

  /**
   * @param message the exception message
   */
  public CancelledException(String message) {
    super(STATUS.withDescription(message));
  }

  /**
   * @param cause the cause of the exception
   */
  public CancelledException(Throwable cause) {
    super(STATUS.withDescription(cause.getMessage()).withCause(cause));
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public CancelledException(String message, Throwable cause) {
    super(STATUS.withDescription(message).withCause(cause));
  }
}
