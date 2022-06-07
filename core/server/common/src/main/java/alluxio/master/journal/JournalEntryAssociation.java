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
import alluxio.proto.journal.Journal.JournalEntry;

/**
 * Association from journal entry to the master it applies to.
 */
public final class JournalEntryAssociation {

  /**
   * @param entry a journal entry
   * @return the name of the master responsible for the given journal entry
   */
  public static String getMasterForEntry(JournalEntry entry) {
    if (entry.hasAddMountPoint()
        || entry.hasAsyncPersistRequest()
        || entry.hasAddSyncPoint()
        || entry.hasActiveSyncTxId()
        || entry.hasCompleteFile()
        || entry.hasDeleteFile()
        || entry.hasDeleteMountPoint()
        || entry.hasInodeDirectory()
        || entry.hasInodeDirectoryIdGenerator()
        || entry.hasInodeFile()
        || entry.hasInodeLastModificationTime()
        || entry.hasNewBlock()
        || entry.hasPersistDirectory()
        || entry.hasRemoveSyncPoint()
        || entry.hasRename()
        || entry.hasSetAcl()
        || entry.hasSetAttribute()
        || entry.hasUpdateUfsMode()
        || entry.hasUpdateInode()
        || entry.hasUpdateInodeDirectory()
        || entry.hasUpdateInodeFile()) {
      return Constants.FILE_SYSTEM_MASTER_NAME;
    }
    if (entry.hasBlockContainerIdGenerator()
        || entry.hasDeleteBlock()
        || entry.hasBlockInfo()) {
      return Constants.BLOCK_MASTER_NAME;
    }
    if (entry.hasClusterInfo()
        || entry.hasPathProperties()
        || entry.hasRemovePathProperties()) {
      return Constants.META_MASTER_NAME;
    }
    if (entry.hasAttachDb()
        || entry.hasAddTable()
        || entry.hasAddTablePartitions()
        || entry.hasRemoveTable()
        || entry.hasDetachDb()
        || entry.hasUpdateDatabaseInfo()
        || entry.hasAddTransformJobInfo()
        || entry.hasRemoveTransformJobInfo()
        || entry.hasCompleteTransformTable()) {
      return Constants.TABLE_MASTER_NAME;
    }
    throw new IllegalStateException("Unrecognized journal entry: " + entry);
  }

  private JournalEntryAssociation() {} // Not intended for instantiation.
}
