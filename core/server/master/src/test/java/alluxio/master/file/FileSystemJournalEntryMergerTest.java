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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.AlluxioURI;
import alluxio.master.block.BlockId;
import alluxio.master.file.meta.PersistenceState;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;

import org.junit.Test;

import java.util.List;

public class FileSystemJournalEntryMergerTest {
  /**
   * Tests if the FileSystemJournalEntryMerger is able to merge inode creations and updates.
   */
  @Test
  public void testFileSystemJournalEntryMerger() {
    AlluxioURI uri = new AlluxioURI("/dir/test1");

    FileSystemJournalEntryMerger merger = new FileSystemJournalEntryMerger();

    merger.add(Journal.JournalEntry.newBuilder().setInodeFile(
        File.InodeFileEntry.newBuilder().setId(
                BlockId.createBlockId(1, BlockId.getMaxSequenceNumber())).setLength(2)
            .setPersistenceState(PersistenceState.PERSISTED.name())
            .setName("test1").setPath(uri.getPath()).build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setInodeFile(
        File.InodeFileEntry.newBuilder().setId(
                BlockId.createBlockId(2, BlockId.getMaxSequenceNumber())).setLength(3)
            .setPersistenceState(PersistenceState.PERSISTED.name())
            .setName("test2").build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setUpdateInode(
        File.UpdateInodeEntry.newBuilder().setId(
                BlockId.createBlockId(3, BlockId.getMaxSequenceNumber()))
            .setName("test3_unchanged").build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setUpdateInode(
        File.UpdateInodeEntry.newBuilder().setId(
                BlockId.createBlockId(2, BlockId.getMaxSequenceNumber()))
            .setName("test2_updated").build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setUpdateInodeFile(
        File.UpdateInodeFileEntry.newBuilder().setId(
                BlockId.createBlockId(1, BlockId.getMaxSequenceNumber()))
            .setLength(200).build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setInodeDirectory(
        File.InodeDirectoryEntry.newBuilder().setId(1).setParentId(0)
            .setPersistenceState(PersistenceState.PERSISTED.name())
            .setName("test_dir").setPath(uri.getPath()).build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setUpdateInodeDirectory(
        File.UpdateInodeDirectoryEntry.newBuilder().setId(1)
            .setDirectChildrenLoaded(true).build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setUpdateInode(
        File.UpdateInodeEntry.newBuilder().setId(1).setName("test_dir_updated").build()).build());

    merger.add(Journal.JournalEntry.newBuilder().setAddMountPoint(
        File.AddMountPointEntry.newBuilder().setMountId(1).build()
    ).build());

    List<Journal.JournalEntry> entries = merger.getMergedJournalEntries();
    Journal.JournalEntry entry = entries.get(0);
    assertNotNull(entry.getInodeFile());
    assertEquals(BlockId.createBlockId(1, BlockId.getMaxSequenceNumber()),
        entry.getInodeFile().getId());
    assertEquals(200, entry.getInodeFile().getLength());
    assertEquals("test1", entry.getInodeFile().getName());

    Journal.JournalEntry entry2 = entries.get(1);
    assertNotNull(entry2.getInodeFile());
    assertEquals(BlockId.createBlockId(2, BlockId.getMaxSequenceNumber()),
        entry2.getInodeFile().getId());
    assertEquals("test2_updated", entry2.getInodeFile().getName());

    Journal.JournalEntry entry3 = entries.get(2);
    assertNotNull(entry3.getUpdateInode());
    assertEquals(BlockId.createBlockId(3, BlockId.getMaxSequenceNumber()),
        entry3.getUpdateInode().getId());
    assertEquals("test3_unchanged", entry3.getUpdateInode().getName());

    Journal.JournalEntry entry4 = entries.get(3);
    assertNotNull(entry4.getInodeDirectory());
    assertEquals(1, entry4.getInodeDirectory().getId());
    assertEquals("test_dir_updated", entry4.getInodeDirectory().getName());

    Journal.JournalEntry entry5 = entries.get(4);
    assertNotNull(entry5.getAddMountPoint());
    assertEquals(1, entry5.getAddMountPoint().getMountId());

    merger.clear();
    assertEquals(0, merger.getMergedJournalEntries().size());
  }
}
