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
 * Exception indicating that operation was rejected because the system is not in a state required
 * for the operation's execution. For example, directory to be deleted may be non-empty, an rmdir
 * operation is applied to a non-directory, etc.
 *
 * A litmus test that may help a service implementor in deciding between
 * FailedPrecondition, Aborted, and Unavailable:
 *
 * <pre>
 *   (a) Use Unavailable if the client can retry just the failing call.
 *   (b) Use Aborted if the client should retry at a higher-level (e.g., restarting a
 *       read-modify-write sequence).
 *   (c) Use FailedPrecondition if the client should not retry until the system state has
 *       been explicitly fixed. E.g., if an "rmdir" fails because the directory is non-empty,
 *       FailedPrecondition should be thrown since the client should not retry unless they
 *       have first fixed up the directory by deleting files from it.
 *   (d) Use FailedPrecondition if the client performs conditional REST Get/Update/Delete
 *       on a resource and the resource on the server does not match the condition. E.g.,
 *       conflicting read-modify-write on the same resource.
 * </pre>
 */
public class FailedPreconditionRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.FAILED_PRECONDITION;
  private static final ErrorType ERROR_TYPE = ErrorType.User;
  private static final boolean RETRYABLE = false;

  /**
   * Constructor.
   * @param t cause
   */
  public FailedPreconditionRuntimeException(Throwable t) {
    super(STATUS, t.getMessage(), t, ERROR_TYPE, RETRYABLE);
  }

  /**
   * Constructor.
   *
   * @param message error message
   */
  public FailedPreconditionRuntimeException(String message) {
    super(STATUS, message, null, ERROR_TYPE, RETRYABLE);
  }

  /**
   * Constructor.
   *
   * @param message error message
   * @param t cause
   */
  public FailedPreconditionRuntimeException(String message, Throwable t) {
    super(STATUS, message, t, ERROR_TYPE, RETRYABLE);
  }
}
