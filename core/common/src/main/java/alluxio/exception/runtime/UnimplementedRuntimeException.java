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
 * Exception indicating that an operation is not implemented or not supported/enabled in this
 * service.
 */
public class UnimplementedRuntimeException extends AlluxioRuntimeException {
  private static final ErrorType ERROR_TYPE = ErrorType.Internal;
  private static final Status STATUS = Status.UNIMPLEMENTED;
  private static final boolean RETRYABLE = false;

  /**
   * Constructor.
   *
   * @param t         cause
   * @param errorType error type
   */
  public UnimplementedRuntimeException(Throwable t, ErrorType errorType) {
    super(STATUS, t.getMessage(), t, errorType, RETRYABLE);
  }

  /**
   * Constructor.
   * @param message error message
   */
  public UnimplementedRuntimeException(String message) {
    super(STATUS, message, null, ERROR_TYPE, RETRYABLE);
  }
}
