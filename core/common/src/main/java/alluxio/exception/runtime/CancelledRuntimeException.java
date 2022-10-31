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

package alluxio.exception.runtime;

import alluxio.grpc.ErrorType;

import io.grpc.Status;

/**
 * Exception indicating that the service is cancelled or interrupted (typically by the caller).
 */
public class CancelledRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.CANCELLED;
  private static final ErrorType ERROR_TYPE = ErrorType.User;
  private static final boolean RETRYABLE = false;

  /**
   * Constructor.
   * @param message error message
   * @param cause cause
   */
  public CancelledRuntimeException(String message, Throwable cause) {
    super(STATUS, message, cause, ERROR_TYPE, RETRYABLE);
  }

  /**
   * Constructor.
   * @param message error message
   */
  public CancelledRuntimeException(String message) {
    super(STATUS, message, null, ERROR_TYPE, RETRYABLE);
  }
}
