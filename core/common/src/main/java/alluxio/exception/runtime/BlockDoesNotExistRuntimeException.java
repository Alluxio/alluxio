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

package alluxio.exception.runtime;

import java.text.MessageFormat;

/**
 * The exception thrown when a block does not exist in Alluxio.
 */
public class BlockDoesNotExistRuntimeException extends NotFoundRuntimeException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public BlockDoesNotExistRuntimeException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param blockId block id
   */
  public BlockDoesNotExistRuntimeException(long blockId) {
    this(MessageFormat.format("BlockMeta not found for blockId {0,number,#}", blockId));
  }
}
