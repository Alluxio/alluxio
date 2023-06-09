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
 * Exception indicating that some resource has been exhausted, perhaps a per-user quota, or perhaps
 * the task queue.
 */
public class ResourceExhaustedRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.RESOURCE_EXHAUSTED;
  private static final ErrorType ERROR_TYPE = ErrorType.Internal;

  /**
   * Constructor.
   * @param message error message
   * @param retryable whether it's retryable
   */
  public ResourceExhaustedRuntimeException(String message, boolean retryable) {
    super(STATUS, message, null, ERROR_TYPE, retryable);
  }

  /**
   * Constructor.
   * @param message error message
   * @param cause cause
   * @param retryable whether it's retryable
   */
  public ResourceExhaustedRuntimeException(String message, Throwable cause, boolean retryable) {
    super(STATUS, message, cause, ERROR_TYPE, retryable);
  }
}
