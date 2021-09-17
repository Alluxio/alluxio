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
 * Exception indicating that a client specified an invalid argument. Note that this differs from
 * FailedPreconditionException. It indicates arguments that are problematic regardless of the state
 * of the system (e.g., a malformed file name).
 */
public class InvalidArgumentException extends AlluxioStatusException {
  private static final long serialVersionUID = -8484299420986871031L;
  private static final Status STATUS = Status.INVALID_ARGUMENT;

  /**
   * @param message the exception message
   */
  public InvalidArgumentException(String message) {
    super(STATUS.withDescription(message));
  }

  /**
   * @param cause the cause of the exception
   */
  public InvalidArgumentException(Throwable cause) {
    super(STATUS.withDescription(cause.getMessage()).withCause(cause));
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public InvalidArgumentException(String message, Throwable cause) {
    super(STATUS.withDescription(message).withCause(cause));
  }
}
