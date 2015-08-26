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

import java.text.MessageFormat;

/**
 * Exception messages used across Tachyon.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
public enum ExceptionMessage {
  // general

  // block manager
  LOCK_ID_FOR_DIFFERENT_BLOCK("lockId {0} is for block {1}, not {2}"),
  LOCK_ID_FOR_DIFFERENT_USER("lockId {0} is owned by userId {1} not {2}"),
  LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_USER("no lock is found for blockId {0} for userId {1}"),
  LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID("lockId {0} has no lock record"),

  // block metadata manager and view
  BLOCK_META_NOT_FOUND("BlockMeta not found for blockId {0}"),
  GET_DIR_FROM_NON_SPECIFIC_LOCATION("Cannot get path from non-specific dir {0}"),
  TEMP_BLOCK_META_NOT_FOUND("TempBlockMeta not found for blockId {0}"),
  TIER_ALIAS_NOT_FOUND("Tier with alias {0} not found"),
  TIER_VIEW_ALIAS_NOT_FOUND("Tier view with alias {0} not found"),

  // SEMICOLON! minimize merge conflicts by putting it on its own line
  ;

  private final MessageFormat mMessage;

  ExceptionMessage(String message) {
    this.mMessage = new MessageFormat(message);
  }

  public String getMessage(Object... params) {
    // MessageFormat is not thread-safe, so guard it
    synchronized (mMessage) {
      return this.mMessage.format(params);
    }
  }
}
