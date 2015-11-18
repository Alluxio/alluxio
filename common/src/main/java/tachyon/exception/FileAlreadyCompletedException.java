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

public class FileAlreadyCompletedException extends TachyonException {
  private static final long serialVersionUID = -623565413624572542L;

  private static final TachyonExceptionType EXCEPTION_TYPE =
      TachyonExceptionType.FILE_ALREADY_COMPLETED;

  public FileAlreadyCompletedException(String message) {
    super(EXCEPTION_TYPE, message);
  }

  public FileAlreadyCompletedException(String message, Throwable cause) {
    super(EXCEPTION_TYPE, message, cause);
  }

  public FileAlreadyCompletedException(ExceptionMessage message, Object... params) {
    this(message.getMessage(params));
  }

  public FileAlreadyCompletedException(ExceptionMessage message,
                                       Throwable cause, Object... params) {
    this(message.getMessage(params), cause);
  }
}
