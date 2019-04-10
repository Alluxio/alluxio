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
 * Exception indicating unrecoverable data loss or corruption.
 */
public class DataLossException extends AlluxioStatusException {
  private static final long serialVersionUID = -4649396237077502949L;
  private static final Status STATUS = Status.DATA_LOSS;

  /**
   * @param message the exception message
   */
  public DataLossException(String message) {
    super(STATUS.withDescription(message));
  }

  /**
   * @param cause the cause of the exception
   */
  public DataLossException(Throwable cause) {
    super(STATUS.withDescription(cause.getMessage()).withCause(cause));
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public DataLossException(String message, Throwable cause) {
    super(STATUS.withDescription(message).withCause(cause));
  }
}
