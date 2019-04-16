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
 * Exception indicating that and operation was attempted past the valid range. E.g., seeking or
 * reading past end of file.
 *
 * Unlike InvalidArgumentException, this error indicates a problem that may be fixed if the system
 * state changes. For example, a 32-bit file system will generate InvalidArgumentException if asked
 * to read at an offset that is not in the range [0,2^32-1], but it will generate
 * OutOfRangeException if asked to read from an offset past the current file size.
 *
 * There is a fair bit of overlap between FailedPreconditionException and OutOfRangeException. We
 * recommend using OutOfRangeException (the more specific error) when it applies so that callers who
 * are iterating through a space can easily look for an OutOfRangeException to detect when they are
 * done.
 */
public class OutOfRangeException extends AlluxioStatusException {
  private static final long serialVersionUID = 5703697898649540073L;
  private static final Status STATUS = Status.OUT_OF_RANGE;

  /**
   * @param message the exception message
   */
  public OutOfRangeException(String message) {
    super(STATUS.withDescription(message));
  }

  /**
   * @param cause the cause of the exception
   */
  public OutOfRangeException(Throwable cause) {
    super(STATUS.withDescription(cause.getMessage()).withCause(cause));
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public OutOfRangeException(String message, Throwable cause) {
    super(STATUS.withDescription(message).withCause(cause));
  }
}
