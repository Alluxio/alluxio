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
    if (entry.hasAddMountPoint()) {
      return entry.getAddMountPoint();
    }
    if (entry.hasAsyncPersistRequest()) {
      return entry.getAsyncPersistRequest();
    }
    if (entry.hasBlockContainerIdGenerator()) {
      return entry.getBlockContainerIdGenerator();
    }
    if (entry.hasBlockInfo()) {
      return entry.getBlockInfo();
    }
    if (entry.hasCompleteFile()) {
      return entry.getCompleteFile();
    }
    if (entry.hasDeleteFile()) {
      return entry.getDeleteFile();
    }
    if (entry.hasDeleteLineage()) {
      return entry.getDeleteLineage();
    }
    if (entry.hasDeleteMountPoint()) {
      return entry.getDeleteMountPoint();
    }
    if (entry.hasInodeDirectory()) {
      return entry.getInodeDirectory();
    }
    if (entry.hasInodeDirectoryIdGenerator()) {
      return entry.getInodeDirectoryIdGenerator();
    }
    if (entry.hasInodeFile()) {
      return entry.getInodeFile();
    }
    if (entry.hasInodeLastModificationTime()) {
      return entry.getInodeLastModificationTime();
    }
    if (entry.hasLineage()) {
      return entry.getLineage();
    }
    if (entry.hasLineageIdGenerator()) {
      return entry.getLineageIdGenerator();
    }
    if (entry.hasPersistDirectory()) {
      return entry.getPersistDirectory();
    }
    if (entry.hasReinitializeFile()) {
      return entry.getReinitializeFile();
    }
    if (entry.hasRename()) {
      return entry.getRename();
    }
    if (entry.hasSetAttribute()) {
      return entry.getSetAttribute();
    }
    if (entry.hasCompleteStore()) {
      return entry.getCreateStore();
    }
    if (entry.hasCompletePartition()) {
      return entry.getCompletePartition();
    }
    if (entry.hasCompleteStore()) {
      return entry.getCompleteStore();
    }
    if (entry.hasDeleteMountPoint()) {
      return entry.getDeleteStore();
    }
    if (entry.hasRenameStore()) {
      return entry.getRenameStore();
    }
    if (entry.hasMergeStore()) {
      return entry.getMergeStore();
    }
    throw new IllegalStateException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY
        .getMessage(entry.getUnknownFields().asMap().keySet().toString()));
  }

  private JournalProtoUtils() {} // not for instantiation
}
