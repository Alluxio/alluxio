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

import io.grpc.Status;

/**
 * Exception indicating that the service is currently unavailable. This is a most likely a transient
 * condition and may be corrected by retrying with a backoff.
 *
 * See litmus test in {@link FailedPreconditionException} for deciding between
 * FailedPreconditionException, AbortedException, and UnavailableException.
 */
public class UnavailableRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.UNAVAILABLE;

  /**
   * Constructor.
   * @param message error message
   * @param t cause
   */
  public UnavailableRuntimeException(String message, Throwable t) {
    super(STATUS, message, t, true);
  }
}
