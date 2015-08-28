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
 * Exception used when some entity that we attempted to create (e.g., block, file, directory or
 * lock) already exists.
 */
public final class AlreadyExistsException extends AbstractTachyonException {
  private static final long serialVersionUID = -8538037207176988241L;

  public AlreadyExistsException(String message) {
    super(message);
  }

  public AlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public AlreadyExistsException(ExceptionMessage message, Object... params) {
    super(message, params);
  }

  public AlreadyExistsException(ExceptionMessage message, Throwable cause, Object... params) {
    super(message, cause, params);
  }
}
