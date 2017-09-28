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

import alluxio.Constants;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.proto.journal.Journal.JournalEntry;

/**
 * Association from journal entry to the master it applies to.
 */
public final class JournalEntryAssociation {

  /**
   * @param entry a journal entry
   * @return the name of the master responsible for the given journal entry
   * @throws InvalidJournalEntryException if the journal entry is unrecognized
   */
  public static String getMasterForEntry(JournalEntry entry) throws InvalidJournalEntryException {
    if (entry.hasAddMountPoint()
        || entry.hasAsyncPersistRequest()
        || entry.hasCompleteFile()
        || entry.hasDeleteFile()
        || entry.hasDeleteMountPoint()
        || entry.hasInodeDirectory()
        || entry.hasInodeDirectoryIdGenerator()
        || entry.hasInodeFile()
        || entry.hasInodeLastModificationTime()
        || entry.hasPersistDirectory()
        || entry.hasRename()
        || entry.hasReinitializeFile()
        || entry.hasSetAttribute()) {
      return Constants.FILE_SYSTEM_MASTER_NAME;
    }
    if (entry.hasBlockContainerIdGenerator()
        || entry.hasDeleteBlock()
        || entry.hasBlockInfo()) {
      return Constants.BLOCK_MASTER_NAME;
    }
    if (entry.hasCompletePartition()
        || entry.hasCompleteStore()
        || entry.hasCreateStore()
        || entry.hasDeleteStore()
        || entry.hasRenameStore()
        || entry.hasMergeStore()) {
      return Constants.KEY_VALUE_MASTER_NAME;
    }
    if (entry.hasDeleteLineage()
        || entry.hasLineage()) {
      return Constants.LINEAGE_MASTER_NAME;
    }
    throw new InvalidJournalEntryException("Unrecognized journal entry: " + entry);
  }

  private JournalEntryAssociation() {} // Not intended for instantiation.
}
