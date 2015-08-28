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
 * Exception used when the system is not in a state required for the operation.
 *
 * For example:
 * <ul>
 * <li>userId or blockId does not correspond to that in the record of lockId</li>
 * <li>user A wants to commit a temp block owned by user B</li>
 * </ul>
 */
public final class InvalidStateException extends AbstractTachyonException {
  private static final long serialVersionUID = -7966393090688326795L;

  public InvalidStateException(String message) {
    super(message);
  }

  public InvalidStateException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidStateException(ExceptionMessage message, Object... params) {
    super(message, params);
  }

  public InvalidStateException(ExceptionMessage message, Throwable cause, Object... params) {
    super(message, cause, params);
  }
}
