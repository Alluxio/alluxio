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

import java.io.IOException;

/**
 * Alluxio exception for UFS.
 */
public class AlluxioUfsException extends AlluxioRuntimeException {

  /**
   * Converts a UFS exception to a corresponding {@link AlluxioUfsException}.
   *
   * @param e UFS exception
   * @return alluxio UFS exception
   */
  public static AlluxioUfsException fromUfsException(IOException e) {
    return new AlluxioUfsException(Status.UNKNOWN, e.getMessage(), e, ErrorType.External, false);
  }

  AlluxioUfsException(Status status, String message, Throwable cause, ErrorType errorType,
      boolean retryable) {
    super(status, message, cause, errorType, retryable);
  }
}
