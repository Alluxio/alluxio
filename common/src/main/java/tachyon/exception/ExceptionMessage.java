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

import com.google.common.base.Preconditions;

/**
 * Exception messages used across Tachyon.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
public enum ExceptionMessage {
  // general
  PATH_DOES_NOT_EXIST("Path {0} does not exist"),

  // block lock manager
  LOCK_ID_FOR_DIFFERENT_BLOCK("lockId {0} is for block {1}, not {2}"),
  LOCK_ID_FOR_DIFFERENT_SESSION("lockId {0} is owned by sessionId {1} not {2}"),
  LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_SESSION("no lock is found for blockId {0} for sessionId {1}"),
  LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID("lockId {0} has no lock record"),

  // block metadata manager and view
  BLOCK_META_NOT_FOUND("BlockMeta not found for blockId {0}"),
  GET_DIR_FROM_NON_SPECIFIC_LOCATION("Cannot get path from non-specific dir {0}"),
  TEMP_BLOCK_META_NOT_FOUND("TempBlockMeta not found for blockId {0}"),
  TIER_ALIAS_NOT_FOUND("Tier with alias {0} not found"),
  TIER_VIEW_ALIAS_NOT_FOUND("Tier view with alias {0} not found"),

  // storageDir
  ADD_EXISTING_BLOCK("blockId {0} exists in {1}"),
  BLOCK_NOT_FOUND_FOR_SESSION("blockId {0} in {1} not found for session {2}"),
  NO_SPACE_FOR_BLOCK_META("blockId {0} is {1} bytes, but only {2} bytes available in {3}"),

  // tieredBlockStore
  BLOCK_ID_FOR_DIFFERENT_SESSION("BlockId {0} is owned by sessionId {1} not {2}"),
  BLOCK_NOT_FOUND_AT_LOCATION("Block {0} not found at location: {1}"),
  MOVE_UNCOMMITTED_BLOCK("Cannot move uncommitted block {0}"),
  NO_BLOCK_ID_FOUND("BlockId {0} not found"),
  NO_EVICTION_PLAN_TO_FREE_SPACE("No eviction plan by evictor to free space"),
  NO_SPACE_FOR_BLOCK_ALLOCATION("Failed to allocate {0} bytes after {1} retries for blockId {2}"),
  NO_SPACE_FOR_BLOCK_MOVE("Failed to find space in {0} to move blockId {1} after {2} retries"),
  REMOVE_UNCOMMITTED_BLOCK("Cannot remove uncommitted block {0}"),
  TEMP_BLOCK_ID_COMMITTED("Temp blockId {0} is not available, because it is already committed"),
  TEMP_BLOCK_ID_EXISTS("Temp blockId {0} is not available, because it already exists"),

  // file system master
  UNEXPECETD_JOURNAL_ENTRY("Unexpected entry in journal: {0}"),
  FILEID_MUST_BE_FILE("File id {0} must be a file"),
  JOURNAL_WRITE_AFTER_CLOSE("Cannot write entry after closing the stream"),
  RAW_TABLE_ID_DOES_NOT_EXIST("Raw table with id {0} does not exist"),
  UNKNOWN_ENTRY_TYPE("Unknown entry type: {0}"),

  // lineage
  LINEAGE_INPUT_FILE_NOT_EXIST("The lineage input file {0} does not exist"),

  // SEMICOLON! minimize merge conflicts by putting it on its own line
  ;

  private final MessageFormat mMessage;

  ExceptionMessage(String message) {
    mMessage = new MessageFormat(message);
  }

  public String getMessage(Object... params) {
    Preconditions.checkArgument(mMessage.getFormats().length == params.length, "The message takes "
        + mMessage.getFormats().length + " arguments, but is given " + params.length);
    // MessageFormat is not thread-safe, so guard it
    synchronized (mMessage) {
      return mMessage.format(params);
    }
  }
}
