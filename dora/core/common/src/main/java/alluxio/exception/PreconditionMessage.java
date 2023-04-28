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
 * Precondition messages used across Alluxio.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
@ThreadSafe
public enum PreconditionMessage {
  EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK(
      "The passed-in file info list can not be empty when checking permission"),
  ERR_BUFFER_STATE("Buffer length: %s, offset: %s, len: %s"),
  ERR_READ_BUFFER_NULL("Read buffer cannot be null"),
  ERR_SEEK_NEGATIVE("Seek position is negative: %s"),
  ERR_SEEK_PAST_END_OF_FILE("Seek position past end of file: %s"),
  ERR_WRITE_BUFFER_NULL("Cannot write a null input buffer"),
  BLOCK_WRITE_LOCATION_POLICY_UNSPECIFIED("The location policy is not specified"),
  // SEMICOLON! minimize merge conflicts by putting it on its own line
  ;

  private final String mMessage;

  PreconditionMessage(String message) {
    mMessage = message;
  }

  @Override
  public String toString() {
    return mMessage;
  }
}
