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
 * Exception representing an internal error. This means some invariant expected by the underlying
 * system has been broken. If you see one of these errors, something is very broken.
 */
public class InternalRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.INTERNAL;
  private static final ErrorType ERROR_TYPE = ErrorType.Internal;
  private static final boolean RETRYABLE = false;

  /**
   * Constructor.
   * @param message error message
   * @param t cause
   */
  public InternalRuntimeException(String message, Throwable t) {
    super(STATUS, message, t, ERROR_TYPE, RETRYABLE);
  }

  /**
   * Constructor.
   * @param message error message
   */
  public InternalRuntimeException(String message) {
    super(STATUS, message, null, ERROR_TYPE, RETRYABLE);
  }

    /**
   * Constructor.
   * @param t cause
   */
  public InternalRuntimeException(Throwable t) {
    super(STATUS, t.getMessage(), t, ERROR_TYPE, RETRYABLE);
  }
}
