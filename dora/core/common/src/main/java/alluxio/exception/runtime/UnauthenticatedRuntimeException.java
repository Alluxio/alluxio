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
 * Exception indicating that the request does not have valid authentication credentials for the
 * operation.
 */
public class UnauthenticatedRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.UNAUTHENTICATED;
  private static final ErrorType ERROR_TYPE = ErrorType.User;
  private static final boolean RETRYABLE = false;

  /**
   * Constructor.
   * @param t cause
   */
  public UnauthenticatedRuntimeException(Throwable t) {
    super(STATUS, t.getMessage(), t, ERROR_TYPE, RETRYABLE);
  }
}
