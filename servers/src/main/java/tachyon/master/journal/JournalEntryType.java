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

import tachyon.master.block.journal.BlockContainerIdGeneratorEntry;
import tachyon.master.block.journal.BlockInfoEntry;
import tachyon.master.file.journal.AddMountPointEntry;
import tachyon.master.file.journal.CompleteFileEntry;
import tachyon.master.file.journal.DeleteFileEntry;
import tachyon.master.file.journal.DeleteMountPointEntry;
import tachyon.master.file.journal.InodeDirectoryEntry;
import tachyon.master.file.journal.InodeDirectoryIdGeneratorEntry;
import tachyon.master.file.journal.InodeFileEntry;
import tachyon.master.file.journal.InodeLastModificationTimeEntry;
import tachyon.master.file.journal.PersistDirectoryEntry;
import tachyon.master.file.journal.PersistFileEntry;
import tachyon.master.file.journal.ReinitializeFileEntry;
import tachyon.master.file.journal.RenameEntry;
import tachyon.master.file.journal.SetPinnedEntry;
import tachyon.master.lineage.journal.AsyncCompleteFileEntry;
import tachyon.master.lineage.journal.DeleteLineageEntry;
import tachyon.master.lineage.journal.LineageEntry;
import tachyon.master.lineage.journal.LineageIdGeneratorEntry;
import tachyon.master.lineage.journal.PersistFilesEntry;
import tachyon.master.lineage.journal.RequestFilePersistenceEntry;
import tachyon.master.rawtable.journal.RawTableEntry;
import tachyon.master.rawtable.journal.UpdateMetadataEntry;

/**
 * The types of entries that can be represented in the journal.
 */
public enum JournalEntryType {

  // Block master entries
  BLOCK_CONTAINER_ID_GENERATOR(BlockContainerIdGeneratorEntry.class),
  BLOCK_INFO(BlockInfoEntry.class) ,

  // File system master entries
  ADD_CHECKPOINT(PersistFileEntry.class),
  ADD_MOUNTPOINT(AddMountPointEntry.class),
  COMPLETE_FILE(CompleteFileEntry.class),
  DELETE_FILE(DeleteFileEntry.class),
  DELETE_MOUNTPOINT(DeleteMountPointEntry.class),
  //it is never used
//  FREE,
  INODE_FILE(InodeFileEntry.class),
  INODE_DIRECTORY(InodeDirectoryEntry.class),
  INODE_DIRECTORY_ID_GENERATOR(InodeDirectoryIdGeneratorEntry.class),
  INODE_MTIME(InodeLastModificationTimeEntry.class),
  INODE_PERSISTED(PersistDirectoryEntry.class),
  REINITIALIZE_FILE(ReinitializeFileEntry.class),
  RENAME(RenameEntry.class),
  SET_PINNED(SetPinnedEntry.class),

  // Raw table master entries
  RAW_TABLE(RawTableEntry.class),
  UPDATE_METADATA(UpdateMetadataEntry.class),

  // Lineage master entries
  ASYNC_COMPLETE_FILE(AsyncCompleteFileEntry.class),
  DELETE_LINEAGE(DeleteLineageEntry.class),
  LINEAGE(LineageEntry.class),
  LINEAGE_ID_GENERATOR(LineageIdGeneratorEntry.class),
  PERSIST_FILES(PersistFilesEntry.class),
  REQUEST_FILE_PERSISTENCE(RequestFilePersistenceEntry.class);

  private Class<? extends JournalEntry> mClass;

  JournalEntryType(Class<? extends JournalEntry> clazz) {
    mClass = clazz;
  }

  public Class<? extends JournalEntry> getClazz() {
    return mClass;
  }
}
