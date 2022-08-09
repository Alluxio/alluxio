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
 * Exception representing an unknown error. An example of where this exception may be thrown is if a
 * Status value received from another address space belongs to an error-space that is not known in
 * this address space. Also errors raised by APIs that do not return enough error information may be
 * converted to this error.
 */
public class UnknownRuntimeException extends AlluxioRuntimeException {
  private static final Status STATUS = Status.UNKNOWN;
  private static final ErrorType ERROR_TYPE = ErrorType.Internal;
  private static final boolean RETRYABLE = false;

  /**
   * Constructor.
   * @param t cause
   */
  public UnknownRuntimeException(Throwable t) {
    super(STATUS, null, t, ERROR_TYPE, RETRYABLE);
  }
}
