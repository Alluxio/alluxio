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
 * Exception indicating that an operation is not implemented or not supported/enabled in this
 * service.
 */
public class UnimplementedException extends AlluxioStatusException {
  private static final long serialVersionUID = 5175271592155540983L;
  private static final Status STATUS = Status.UNIMPLEMENTED;

  /**
   * @param message the exception message
   */
  public UnimplementedException(String message) {
    super(STATUS, message);
  }

  /**
   * @param cause the cause of the exception
   */
  public UnimplementedException(Throwable cause) {
    super(STATUS, cause);
  }

  /**
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public UnimplementedException(String message, Throwable cause) {
    super(STATUS, message, cause);
  }
}
