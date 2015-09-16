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
  WORKER_ID_GENERATOR,

  // File system master entries
  INODE_FILE,
  INODE_DIRECTORY,
  INODE_MTIME,
  ADD_CHECKPOINT,
  DEPENDENCY,
  COMPLETE_FILE,
  FREE,
  SET_PINNED,
  DELETE_FILE,
  RENAME,
  INODE_DIRECTORY_ID_GENERATOR,

  // Raw table master entries
  RAW_TABLE,
  UPDATE_METADATA,
}
