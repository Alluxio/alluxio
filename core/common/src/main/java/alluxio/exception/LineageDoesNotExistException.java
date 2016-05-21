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
 * The exception thrown when a lineage does not exist in Alluxio.
 */
@ThreadSafe
public class LineageDoesNotExistException extends AlluxioException {
  private static final long serialVersionUID = 6099440428939973308L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public LineageDoesNotExistException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public LineageDoesNotExistException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified exception message and multiple parameters.
   *
   * @param message the exception message
   * @param params the parameters
   */
  public LineageDoesNotExistException(ExceptionMessage message, Object... params) {
    this(message.getMessage(params));
  }

  /**
   * Constructs a new exception with the specified exception message, the cause and multiple
   * parameters.
   *
   * @param message the exception message
   * @param cause the cause
   * @param params the parameters
   */
  public LineageDoesNotExistException(ExceptionMessage message, Throwable cause, Object... params) {
    this(message.getMessage(params), cause);
  }

  /**
   * Ensures the truth of an expression involving the state of the calling instance, but not
   * involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param message {@link ExceptionMessage} template should the check fail
   * @param params the arguments to be substituted into the message template. Arguments are
   *               converted to strings using {@link ExceptionMessage#getMessage(Object...)}.
   * @throws LineageDoesNotExistException if {@code expression} is false
   */
  public static void check(boolean expression, ExceptionMessage message, Object... params)
      throws LineageDoesNotExistException {
    if (!expression) {
      throw new LineageDoesNotExistException(message, params);
    }
  }
}
