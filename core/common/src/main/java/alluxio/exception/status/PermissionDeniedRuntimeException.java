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

import alluxio.exception.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;

import io.grpc.Status;

/**
 * Exception indicating that the caller does not have permission to execute the specified operation.
 * It must not be used for rejections caused by exhausting some resource (use
 * ResourceExhaustedException instead for those exceptions). It must not be used if the caller
 * cannot be identified (use UnauthenticatedException instead for those exceptions).
 */
public class PermissionDeniedRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.PERMISSION_DENIED;
  private static final ErrorType ERROR_TYPE = ErrorType.User;
  private static final boolean RETRYABLE = false;

  /**
   * Constructor.
   * @param t error message
   */
  public PermissionDeniedRuntimeException(Throwable t) {
    super(STATUS, null, t, ERROR_TYPE, RETRYABLE);
  }
}
