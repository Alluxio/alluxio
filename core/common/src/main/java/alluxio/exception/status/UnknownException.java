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

/**
 * Exception representing an unknown error. An example of where this exception may be thrown is if a
 * Status value received from another address space belongs to an error-space that is not known in
 * this address space. Also errors raised by APIs that do not return enough error information may be
 * converted to this error.
 */
public class UnknownException extends AlluxioStatusException {
  private static final long serialVersionUID = 1310329692937489461L;
  private static final Status STATUS = Status.UNKNOWN;

  /**
   * @param message the exception message
   */
  public UnknownException(String message) {
    super(STATUS, message);
  }

  /**
   * @param cause the cause of the exception
   */
  public UnknownException(Throwable cause) {
    super(STATUS, cause);
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public UnknownException(String message, Throwable cause) {
    super(STATUS, message, cause);
  }
}
