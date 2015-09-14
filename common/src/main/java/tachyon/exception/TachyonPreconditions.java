/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.exception;

/**
 * Simple static methods to be called at the start of your own Tachyon methods to verify
 * correct arguments and Tachyon system state. This allows constructs such as
 * <pre>
 *    if (mMetaManager.hasTempBlockMeta(blockId)) {
 *      throw new AlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_EXISTS, blockId);
 *    }</pre>
 *
 * to be replaced with the more compact
 * <pre>
 *     TachyonPreconditions.checkNotExist(!mMetaManager.hasTempBlockMeta(blockId),
 *        ExceptionMessage.TEMP_BLOCK_ID_EXISTS, blockId);</pre>
 *
 * Note that similar to {@code Preconditions}, the sense of the expression is inverted;
 * you declare what you expect to be <i>true</i>, just as you do with an
 * <a href="http://java.sun.com/j2se/1.5.0/docs/guide/language/assert.html">
 * {@code assert}</a> or a JUnit {@code assertTrue} call.
 *
 * <p>Take care not to confuse precondition checking with other similar types
 * of checks! Precondition exceptions -- including those provided here, but also
 * {@link IndexOutOfBoundsException}, {@link java.util.NoSuchElementException}, {@link
 * UnsupportedOperationException} and others -- are used to signal that the
 * <i>calling method</i> has made an error. This tells the caller that it should
 * not have invoked the method when it did, with the arguments it did, or
 * perhaps ever. Postcondition or other invariant failures should not throw
 * these types of exceptions.
 */

public final class TachyonPreconditions{
  private TachyonPreconditions() {}

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @throws AlreadyExistsException if {@code expression} is false
   */
  public static void checkNotExist(boolean expression) throws AlreadyExistsException {
    if (!expression) {
      throw new AlreadyExistsException("");
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param message an exception message should the check fail.
   * @throws AlreadyExistsException if {@code expression} is false
   */
  public static void checkNotExist(
      boolean expression, String message) throws AlreadyExistsException {
    if (!expression) {
      throw new AlreadyExistsException(message);
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param message an exception message should the check fail.
   * @param params the arguments to be substituted into the message
   * @throws AlreadyExistsException if {@code expression} is false
   */
  public static void checkNotExist(boolean expression, ExceptionMessage message, Object... params)
    throws AlreadyExistsException {
    if (!expression) {
      throw new AlreadyExistsException(message, params);
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param message an exception message should the check fail.
   * @param cause a throwable object indicating the cause (which is saved for later retrieval by
   *              the Throwable.getCause() method)
   * @param params the arguments to be substituted into the message
   * @throws AlreadyExistsException if {@code expression} is false
   */
  public static void checkNotExist(boolean expression,
                                   ExceptionMessage message, Throwable cause, Object... params)
      throws AlreadyExistsException {
    if (!expression) {
      throw new AlreadyExistsException(message, cause, params);
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @throws InvalidStateException if {@code expression} is false
   */
  public static void checkState(boolean expression)
      throws InvalidStateException {
    if (!expression) {
      throw new InvalidStateException("");
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param message an exception message should the check fail.
   * @throws InvalidStateException if {@code expression} is false
   */
  public static void checkState(boolean expression, ExceptionMessage message)
      throws InvalidStateException {
    if (!expression) {
      throw new InvalidStateException(message);
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param message an exception message should the check fail.
   * @param params the arguments to be substituted into the message
   * @throws InvalidStateException if {@code expression} is false
   */
  public static void checkState(boolean expression, ExceptionMessage message, Object... params)
      throws InvalidStateException {
    if (!expression) {
      throw new InvalidStateException(message, params);
    }
  }

  /**
   * Ensures the truth of an expression involving the state of the calling
   * instance, but not involving any parameters to the calling method.
   *
   * @param expression a boolean expression
   * @param message an exception message should the check fail.
   * @param cause a throwable object indicating the cause (which is saved for later retrieval by
   *              the Throwable.getCause() method)
   * @param params the arguments to be substituted into the message
   * @throws InvalidStateException if {@code expression} is false
   */
  public static void checkState(boolean expression,
                                ExceptionMessage message, Throwable cause, Object... params)
      throws InvalidStateException {
    if (!expression) {
      throw new InvalidStateException(message, cause, params);
    }
  }
}