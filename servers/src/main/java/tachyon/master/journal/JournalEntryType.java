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

package tachyon.master.journal;

/**
 * The types of entries that can be represented in the journal.
 */
public enum JournalEntryType {
  // Block master entries
  BLOCK_CONTAINER_ID_GENERATOR,
  BLOCK_INFO,

  // File system master entries
  ADD_CHECKPOINT,
  ADD_MOUNTPOINT,
  COMPLETE_FILE,
  DELETE_FILE,
  DELETE_MOUNTPOINT,
  FREE,
  INODE_FILE,
  INODE_DIRECTORY,
  INODE_DIRECTORY_ID_GENERATOR,
  INODE_MTIME,
  INODE_PERSISTED,
  REINITIALIZE_FILE,
  RENAME,
  SET_PINNED,
  SET_TTL,

  // Raw table master entries
  RAW_TABLE,
  UPDATE_METADATA,

  // Lineage master entries
  ASYNC_COMPLETE_FILE,
  DELETE_LINEAGE,
  LINEAGE,
  LINEAGE_ID_GENERATOR,
  PERSIST_FILES,
  REQUEST_FILE_PERSISTENCE,
}
