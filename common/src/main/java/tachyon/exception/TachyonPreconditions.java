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
 * Simple static methods to be called at the start of your own Tachyon methods to verify correct
 * arguments and Tachyon system state. This allows constructs such as
 * TODO(zhongyi): update example before open PR
 * <pre>
 * if (mMetaManager.hasTempBlockMeta(blockId)) {
 *   throw new AlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_EXISTS, blockId);
 * }
 * </pre>
 *
 * to be replaced with the more compact
 *
 * <pre>
 * TachyonPreconditions.checkNotExist(!mMetaManager.hasTempBlockMeta(blockId),
 *     ExceptionMessage.TEMP_BLOCK_ID_EXISTS, blockId);
 * </pre>
 *
 * Note that similar to {@code Preconditions}, the sense of the expression is inverted; you declare
 * what you expect to be <i>true</i>, just as you do with an
 * <a href="http://java.sun.com/j2se/1.5.0/docs/guide/language/assert.html"> {@code assert}</a> or a
 * JUnit {@code assertTrue} call.
 *
 */

public final class TachyonPreconditions {
  private TachyonPreconditions() {}

  /**
   * Ensures that the lineage exist by checking the truth of the expression
   *
   * @param expression a boolean expression
   * @param errorMessageTemplate a template for the exception message should the check fail
   * @param params the arguments to be substituted into the message template
   * @throws LineageDoesNotExistException if {@code expression} is false
   */
  public static void CheckLineageExist(boolean expression, String errorMessageTemplate,
      Object... params) throws LineageDoesNotExistException {
    if (!expression) {
      throw new LineageDoesNotExistException(String.format(errorMessageTemplate, params));
    }
  }

  /**
   * Ensures that the lineage exist by checking the truth of the expression
   *
   * @param expression a boolean expression
   * @param params the arguments to be substituted into the default message
   *        {@Link PreconditionMessage#LINEAGE_DOES_NOT_EXIST}
   * @throws LineageDoesNotExistException if {@code expression} is false
   */
  public static void CheckLineageExist(boolean expression, Object... params)
      throws LineageDoesNotExistException {
    CheckLineageExist(expression, PreconditionMessage.LINEAGE_DOES_NOT_EXIST, params);
  }

  /**
   * Ensures that the lineage exist by checking the truth of the expression
   *
   * @param expression a boolean expression
   * @param errorMessageTemplate a template for the exception message should the check fail
   * @param cause a throwable object indicating the cause (which is saved for later retrieval by the
   *        Throwable.getCause() method)
   * @param params the arguments to be substituted into the message template
   * @throws LineageDoesNotExistException if {@code expression} is false
   */
  public static void CheckLineageExist(boolean expression, String errorMessageTemplate,
      Throwable cause, Object... params) throws LineageDoesNotExistException {
    if (!expression) {
      throw new LineageDoesNotExistException(
          String.format(errorMessageTemplate, params), cause);
    }
  }

}
