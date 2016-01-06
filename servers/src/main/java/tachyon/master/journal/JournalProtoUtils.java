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

import com.google.protobuf.Message;

import tachyon.exception.ExceptionMessage;
import tachyon.proto.journal.Journal.JournalEntry;

/**
 * Utils for working with the journal.
 */
public final class JournalProtoUtils {
  /**
   * Returns the journal entry wrapped by the given {@link JournalEntry}.
   *
   * @param entry the journal entry to unwrap
   * @return the specific entry wrapped within the given {@link JournalEntry}
   */
  public static Message unwrap(JournalEntry entry) {
    switch (entry.getEntryCase()) {
      case ADD_MOUNT_POINT:
        return entry.getAddMountPoint();
      case BLOCK_CONTAINER_ID_GENERATOR:
        return entry.getBlockContainerIdGenerator();
      case BLOCK_INFO:
        return entry.getBlockInfo();
      case COMPLETE_FILE:
        return entry.getCompleteFile();
      case DELETE_FILE:
        return entry.getDeleteFile();
      case DELETE_LINEAGE:
        return entry.getDeleteLineage();
      case DELETE_MOUNT_POINT:
        return entry.getDeleteMountPoint();
      case INODE_DIRECTORY:
        return entry.getInodeDirectory();
      case INODE_DIRECTORY_ID_GENERATOR:
        return entry.getInodeDirectoryIdGenerator();
      case INODE_FILE:
        return entry.getInodeFile();
      case INODE_LAST_MODIFICATION_TIME:
        return entry.getInodeLastModificationTime();
      case LINEAGE:
        return entry.getLineage();
      case LINEAGE_ID_GENERATOR:
        return entry.getLineageIdGenerator();
      case PERSIST_DIRECTORY:
        return entry.getPersistDirectory();
      case PERSIST_FILE:
        return entry.getPersistFile();
      case PERSIST_FILES_REQUEST:
        return entry.getPersistFilesRequest();
      case RAW_TABLE:
        return entry.getRawTable();
      case REINITIALIZE_FILE:
        return entry.getReinitializeFile();
      case RENAME:
        return entry.getRename();
      case SET_ACL:
        return entry.getSetAcl();
      case SET_STATE:
        return entry.getSetState();
      case UPDATE_METADATA:
        return entry.getUpdateMetadata();
      case ENTRY_NOT_SET:
        // This could mean that the field was never set, or it was set with a different version of
        // this message. Given the history of the JournalEntry protobuf message, the keys of the
        // unknown fields should be enough to figure out which version of JournalEntry is needed to
        // understand this journal.
        throw new RuntimeException(
            ExceptionMessage.NO_ENTRY_TYPE.getMessage(entry.getUnknownFields().asMap().keySet()));
      default:
        throw new IllegalStateException(
            ExceptionMessage.UNKNOWN_JOURNAL_ENTRY_TYPE.getMessage(entry.getEntryCase()));
    }
  }

  private JournalProtoUtils() {} // not for instantiation
}
