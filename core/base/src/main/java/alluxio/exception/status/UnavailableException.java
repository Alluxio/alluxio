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
 * Exception indicating that the service is currently unavailable. This is a most likely a transient
 * condition and may be corrected by retrying with a backoff.
 *
 * See litmus test in {@link FailedPreconditionException} for deciding between
 * FailedPreconditionException, AbortedException, and UnavailableException.
 */
public class UnavailableException extends AlluxioStatusException {
  private static final long serialVersionUID = -8183502434544959673L;
  private static final Status STATUS = Status.UNAVAILABLE;

  /**
   * @param message the exception message
   */
  public UnavailableException(String message) {
    super(STATUS.withDescription(message));
  }

  /**
   * @param cause the cause of the exception
   */
  public UnavailableException(Throwable cause) {
    super(STATUS.withDescription(cause.getMessage()).withCause(cause));
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public UnavailableException(String message, Throwable cause) {
    super(STATUS.withDescription(message).withCause(cause));
  }
}
