/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal;

import alluxio.exception.ExceptionMessage;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.protobuf.Message;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utils for working with the journal.
 */
@ThreadSafe
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
      case ASYNC_PERSIST_REQUEST:
        return entry.getAsyncPersistRequest();
      case REINITIALIZE_FILE:
        return entry.getReinitializeFile();
      case RENAME:
        return entry.getRename();
      case SET_ATTRIBUTE:
        return entry.getSetAttribute();
      case CREATE_STORE:
        return entry.getCreateStore();
      case COMPLETE_PARTITION:
        return entry.getCompletePartition();
      case COMPLETE_STORE:
        return entry.getCompleteStore();
      case DELETE_STORE:
        return entry.getDeleteStore();
      case RENAME_STORE:
        return entry.getRenameStore();
      case MERGE_STORE:
        return entry.getMergeStore();
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
