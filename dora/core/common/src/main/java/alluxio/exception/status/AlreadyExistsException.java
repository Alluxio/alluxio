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
 * Exception indicating that an attempt to create an entity failed because one already exists.
 */
public class AlreadyExistsException extends AlluxioStatusException {
  private static final long serialVersionUID = 2565420784789460508L;
  private static final Status STATUS = Status.ALREADY_EXISTS;

  /**
   * @param message the exception message
   */
  public AlreadyExistsException(String message) {
    super(STATUS.withDescription(message));
  }

  /**
   * @param cause the cause of the exception
   */
  public AlreadyExistsException(Throwable cause) {
    super(STATUS.withDescription(cause.getMessage()).withCause(cause));
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public AlreadyExistsException(String message, Throwable cause) {
    super(STATUS.withDescription(message).withCause(cause));
  }
}
