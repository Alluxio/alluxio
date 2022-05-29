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

package alluxio.exception;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The exception thrown when a worker is in an invalid state, i.e. the id of a block does not belong
 * to the session id.
 */
@ThreadSafe
public class InvalidWorkerStateException extends AlluxioException {
  private static final long serialVersionUID = -5416224494611880597L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public InvalidWorkerStateException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified exception message and multiple parameters.
   *
   * @param message the exception message
   * @param params the parameters
   */
  public InvalidWorkerStateException(ExceptionMessage message, Object... params) {
    this(message.getMessage(params));
  }
}
