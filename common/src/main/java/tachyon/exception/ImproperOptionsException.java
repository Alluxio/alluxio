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
 * Exception for situations where an operation fails, but would have succeeded if the proper options
 * were specified. An example of this would be deleting a nonempty directory without specifying
 * 'recursive'.
 */
public class ImproperOptionsException extends TachyonException {
  private static final long serialVersionUID = -8411287237224325573L;

  private static final TachyonExceptionType EXCEPTION_TYPE =
      TachyonExceptionType.IMPROPER_OPTIONS_EXCEPTION;

  public ImproperOptionsException(String message) {
    super(EXCEPTION_TYPE, message);
  }

  public ImproperOptionsException(String message, Throwable cause) {
    super(EXCEPTION_TYPE, message, cause);
  }

  public ImproperOptionsException(ExceptionMessage message, Object... params) {
    this(message.getMessage(params));
  }

  public ImproperOptionsException(ExceptionMessage message, Throwable cause,
      Object... params) {
    this(message.getMessage(params), cause);
  }
}
