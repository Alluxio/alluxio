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
 * Exception indicating that the operation was aborted, typically due to a concurrency issue like
 * sequencer check failures, transaction aborts, etc.
 *
 * See litmus test in {@link FailedPreconditionException} for deciding between
 * {@link FailedPreconditionException}, {@link AbortedException}, and {@link UnavailableException}.
 */
public class AbortedException extends AlluxioStatusException {
  private static final long serialVersionUID = -7705340466444818294L;
  private static final Status STATUS = Status.ABORTED;

  /**
   * @param message the exception message
   */
  public AbortedException(String message) {
    super(STATUS.withDescription(message));
  }

  /**
   * @param cause the cause of the exception
   */
  public AbortedException(Throwable cause) {
    super(STATUS.withDescription(cause.getMessage()).withCause(cause));
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public AbortedException(String message, Throwable cause) {
    super(STATUS.withDescription(message).withCause(cause));
  }
}
