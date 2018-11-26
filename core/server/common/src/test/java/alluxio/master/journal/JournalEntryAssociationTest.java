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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry;
import alluxio.proto.journal.Block.BlockInfoEntry;
import alluxio.proto.journal.Block.DeleteBlockEntry;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.File.AsyncPersistRequestEntry;
import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.DeleteMountPointEntry;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.InodeLastModificationTimeEntry;
import alluxio.proto.journal.File.NewBlockEntry;
import alluxio.proto.journal.File.PersistDirectoryEntry;
import alluxio.proto.journal.File.ReinitializeFileEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAclEntry;
import alluxio.proto.journal.File.SetAttributeEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.File.UpdateUfsModeEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.KeyValue.CompletePartitionEntry;
import alluxio.proto.journal.KeyValue.CompleteStoreEntry;
import alluxio.proto.journal.KeyValue.CreateStoreEntry;
import alluxio.proto.journal.KeyValue.DeleteStoreEntry;
import alluxio.proto.journal.KeyValue.MergeStoreEntry;
import alluxio.proto.journal.KeyValue.RenameStoreEntry;
import alluxio.proto.journal.Lineage.DeleteLineageEntry;
import alluxio.proto.journal.Lineage.LineageEntry;
import alluxio.proto.journal.Lineage.LineageIdGeneratorEntry;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link JournalEntryAssociation}.
 */
public class JournalEntryAssociationTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  // CHECKSTYLE.OFF: LineLengthExceed
  // This list must contain one of every type of journal entry. If you create a new type of
  // journal entry, make sure to add it here.
  private static List<JournalEntry> ENTRIES = Arrays.asList(
      JournalEntry.newBuilder().setAddMountPoint(AddMountPointEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setAsyncPersistRequest(AsyncPersistRequestEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setBlockContainerIdGenerator(BlockContainerIdGeneratorEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setBlockInfo(BlockInfoEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setCompleteFile(CompleteFileEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setCompletePartition(CompletePartitionEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setCompleteStore(CompleteStoreEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setCreateStore(CreateStoreEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setDeleteBlock(DeleteBlockEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setDeleteFile(DeleteFileEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setDeleteLineage(DeleteLineageEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setDeleteMountPoint(DeleteMountPointEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setDeleteStore(DeleteStoreEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setInodeDirectory(InodeDirectoryEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setInodeDirectoryIdGenerator(InodeDirectoryIdGeneratorEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setInodeFile(InodeFileEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setInodeLastModificationTime(InodeLastModificationTimeEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setLineage(LineageEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setLineageIdGenerator(LineageIdGeneratorEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setMergeStore(MergeStoreEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setNewBlock(NewBlockEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setPersistDirectory(PersistDirectoryEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setReinitializeFile(ReinitializeFileEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setRename(RenameEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setRenameStore(RenameStoreEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setSetAcl(SetAclEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setSetAttribute(SetAttributeEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setUpdateUfsMode(UpdateUfsModeEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setUpdateInode(UpdateInodeEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setUpdateInodeDirectory(UpdateInodeDirectoryEntry.getDefaultInstance()).build(),
      JournalEntry.newBuilder().setUpdateInodeFile(UpdateInodeFileEntry.getDefaultInstance()).build()
  );
  // CHECKSTYLE.OFF: LineLengthExceed

  @Test
  public void testUnknown() {
    mThrown.expect(IllegalStateException.class);
    JournalEntryAssociation.getMasterForEntry(JournalEntry.getDefaultInstance());
  }

  @Test
  public void testEntries() {
    for (JournalEntry entry : ENTRIES) {
      assertNotNull(JournalEntryAssociation.getMasterForEntry(entry));
    }
  }

  @Test
  public void testFullCoverage() {
    int expectedNumFields = JournalEntry.getDescriptor().getFields().size();
    // subtract 1 for sequence_number
    expectedNumFields--;
    assertEquals(expectedNumFields, ENTRIES.size());
  }
}
