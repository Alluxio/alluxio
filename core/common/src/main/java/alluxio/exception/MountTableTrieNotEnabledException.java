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
 * The exception thrown when a Trie related method is called but the MountTableTrie is not enabled.
 */
@ThreadSafe
public class MountTableTrieNotEnabledException extends AlluxioException {
  private static final long serialVersionUID = -3162552182000050901L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public MountTableTrieNotEnabledException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public MountTableTrieNotEnabledException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified exception message and multiple parameters.
   *
   * @param message the exception message
   * @param params the parameters
   */
  public MountTableTrieNotEnabledException(ExceptionMessage message, Object... params) {
    this(message.getMessage(params));
  }

  /**
   * Constructs a new exception stating that the MountTableTrie is not enabled but the Trie
   * related methods are called.
   *
   */
  public MountTableTrieNotEnabledException() {
    this(ExceptionMessage.MOUNT_TABLE_TRIE_NOT_ENABLED.getMessage());
  }
}
