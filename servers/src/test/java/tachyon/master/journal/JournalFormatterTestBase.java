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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.proto.journal.Block.BlockContainerIdGeneratorEntry;
import tachyon.proto.journal.Block.BlockInfoEntry;
import tachyon.proto.journal.File.AddMountPointEntry;
import tachyon.proto.journal.File.AsyncPersistRequestEntry;
import tachyon.proto.journal.File.CompleteFileEntry;
import tachyon.proto.journal.File.DeleteFileEntry;
import tachyon.proto.journal.File.DeleteMountPointEntry;
import tachyon.proto.journal.File.InodeDirectoryEntry;
import tachyon.proto.journal.File.InodeDirectoryIdGeneratorEntry;
import tachyon.proto.journal.File.InodeFileEntry;
import tachyon.proto.journal.File.InodeLastModificationTimeEntry;
import tachyon.proto.journal.File.PersistDirectoryEntry;
import tachyon.proto.journal.File.ReinitializeFileEntry;
import tachyon.proto.journal.File.RenameEntry;
import tachyon.proto.journal.File.SetStateEntry;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.proto.journal.KeyValue.CompletePartitionEntry;
import tachyon.proto.journal.KeyValue.CompleteStoreEntry;
import tachyon.proto.journal.KeyValue.CreateStoreEntry;
import tachyon.proto.journal.Lineage.DeleteLineageEntry;
import tachyon.proto.journal.Lineage.LineageEntry;
import tachyon.proto.journal.Lineage.LineageIdGeneratorEntry;
import tachyon.proto.journal.RawTable.RawTableEntry;
import tachyon.proto.journal.RawTable.UpdateMetadataEntry;
import tachyon.security.authorization.PermissionStatus;
import tachyon.util.io.BufferUtils;

/**
 * Base class for testing different {@link JournalFormatter}'s serialization/deserialization
 * correctness of each entry type defined in {@link JournalEntry.EntryCase}.
 * <p>
 * To test an implementation of {@link JournalFormatter}, extend this class and override method
 * {@link #getFormatter()}.
 * <p>
 * See example usage in {@link ProtoBufJournalFormatterTest}.
 */
public abstract class JournalFormatterTestBase {
  protected static final long TEST_CONTAINER_ID = 2011L;
  protected static final long TEST_BLOCK_ID = 2015L;
  protected static final long TEST_FILE_ID = 1L;
  protected static final long TEST_LINEAGE_ID = 1L;
  protected static final String TEST_FILE_NAME = "journalFormatter.test";
  protected static final long TEST_LENGTH_BYTES = 256L;
  protected static final long TEST_BLOCK_SIZE_BYTES = 256L;
  protected static final long TEST_TABLE_ID = 2L;
  protected static final long TEST_OP_TIME_MS = 1409349750338L;
  protected static final long TEST_SEQUENCE_NUMBER = 1945L;
  protected static final TachyonURI TEST_TACHYON_PATH = new TachyonURI("/test/path");
  protected static final long TEST_TTL = 624L;
  protected static final TachyonURI TEST_UFS_PATH = new TachyonURI("hdfs://host:port/test/path");
  protected static final String TEST_JOB_COMMAND = "Command";
  protected static final String TEST_JOB_OUTPUT_PATH = "/test/path";
  protected static final PermissionStatus TEST_PERMISSION_STATUS =
      new PermissionStatus("user1", "group1", (short)0777);
  protected static final String TEST_PERSISTED_STATE = "PERSISTED";
  protected static final String TEST_KEY1 = "test_key1";
  protected static final String TEST_KEY2 = "test_key2";

  protected JournalFormatter mFormatter = getFormatter();
  protected OutputStream mOs;
  protected InputStream mIs;

  // List containing every type of journal entry
  protected static final List<JournalEntry> ENTRIES_LIST;

  static {
    List<JournalEntry> entries = ImmutableList.<JournalEntry>builder()
        .add(
            JournalEntry.newBuilder()
            .setBlockContainerIdGenerator(
                BlockContainerIdGeneratorEntry.newBuilder()
                .setNextContainerId(TEST_CONTAINER_ID))
            .build())
        .add(
            JournalEntry.newBuilder()
            .setBlockInfo(BlockInfoEntry.newBuilder()
                .setBlockId(TEST_BLOCK_ID)
                .setLength(TEST_LENGTH_BYTES))
            .build())
        .add(JournalEntry.newBuilder()
            .setInodeFile(InodeFileEntry.newBuilder()
                .setCreationTimeMs(TEST_OP_TIME_MS)
                .setId(TEST_FILE_ID)
                .setName(TEST_FILE_NAME)
                .setParentId(TEST_FILE_ID)
                .setPersistenceState(TEST_PERSISTED_STATE)
                .setPinned(true)
                .setLastModificationTimeMs(TEST_OP_TIME_MS)
                .setBlockSizeBytes(TEST_BLOCK_SIZE_BYTES)
                .setLength(TEST_LENGTH_BYTES)
                .setCompleted(true)
                .setCacheable(true)
                .addAllBlocks(ContiguousSet.create(
                    Range.closedOpen(TEST_BLOCK_ID, TEST_BLOCK_ID + 10), DiscreteDomain.longs())
                    .asList())
                .setTtl(Constants.NO_TTL)
                .setUserName(TEST_PERMISSION_STATUS.getUserName())
                .setGroupName(TEST_PERMISSION_STATUS.getGroupName())
                .setPermission(TEST_PERMISSION_STATUS.getPermission().toShort()))
            .build())
        .add(JournalEntry.newBuilder()
            .setInodeDirectory(InodeDirectoryEntry.newBuilder()
                .setCreationTimeMs(TEST_OP_TIME_MS)
                .setId(TEST_FILE_ID)
                .setName(TEST_FILE_NAME)
                .setParentId(TEST_FILE_ID)
                .setPersistenceState(TEST_PERSISTED_STATE)
                .setPinned(true)
                .setLastModificationTimeMs(TEST_OP_TIME_MS)
                .setUserName(TEST_PERMISSION_STATUS.getUserName())
                .setGroupName(TEST_PERMISSION_STATUS.getGroupName())
                .setPermission(TEST_PERMISSION_STATUS.getPermission().toShort()))
            .build())
        .add(JournalEntry.newBuilder()
            .setInodeLastModificationTime(InodeLastModificationTimeEntry.newBuilder()
                .setId(TEST_FILE_ID)
                .setLastModificationTimeMs(TEST_OP_TIME_MS))
            .build())
        .add(JournalEntry.newBuilder()
            .setPersistDirectory(PersistDirectoryEntry.newBuilder()
                .setId(TEST_FILE_ID))
            .build())
        .add(
            JournalEntry.newBuilder()
            .setCompleteFile(CompleteFileEntry.newBuilder()
                .addAllBlockIds(Arrays.asList(1L, 2L, 3L))
                .setId(TEST_FILE_ID)
                .setLength(TEST_LENGTH_BYTES)
                .setOpTimeMs(TEST_OP_TIME_MS))
            .build())
        .add(JournalEntry.newBuilder()
            .setDeleteFile(DeleteFileEntry.newBuilder()
                .setId(TEST_FILE_ID)
                .setRecursive(true)
                .setOpTimeMs(TEST_OP_TIME_MS))
            .build())
        .add(JournalEntry.newBuilder()
            .setRename(RenameEntry.newBuilder()
                .setId(TEST_FILE_ID)
                .setDstPath(TEST_FILE_NAME)
                .setOpTimeMs(TEST_OP_TIME_MS))
            .build())
        .add(JournalEntry.newBuilder()
            .setInodeDirectoryIdGenerator(InodeDirectoryIdGeneratorEntry.newBuilder()
                .setContainerId(TEST_CONTAINER_ID)
                .setSequenceNumber(TEST_SEQUENCE_NUMBER))
            .build())
        .add(JournalEntry.newBuilder()
            .setAddMountPoint(AddMountPointEntry.newBuilder()
                .setTachyonPath(TEST_TACHYON_PATH.toString())
                .setUfsPath(TEST_UFS_PATH.toString()))
            .build())
        .add(
            JournalEntry.newBuilder()
            .setDeleteMountPoint(DeleteMountPointEntry.newBuilder()
                .setTachyonPath(TEST_TACHYON_PATH.toString()))
            .build())
        .add(JournalEntry.newBuilder()
            .setRawTable(RawTableEntry.newBuilder()
                .setId(TEST_BLOCK_ID)
                .setColumns(100)
                .setMetadata(ByteString.copyFrom(BufferUtils.getIncreasingByteBuffer(10))))
            .build())
        .add(JournalEntry.newBuilder()
            .setUpdateMetadata(UpdateMetadataEntry.newBuilder()
                .setId(TEST_BLOCK_ID)
                .setMetadata(ByteString.copyFrom(new byte[10])))
            .build())
        .add(JournalEntry.newBuilder()
            .setReinitializeFile(ReinitializeFileEntry.newBuilder()
                .setPath(TEST_FILE_NAME)
                .setBlockSizeBytes(TEST_BLOCK_SIZE_BYTES)
                .setTtl(TEST_TTL))
            .build())
        .add(
            JournalEntry.newBuilder()
            .setDeleteLineage(DeleteLineageEntry.newBuilder()
                .setLineageId(TEST_LINEAGE_ID)
                .setCascade(false))
            .build())
        .add(JournalEntry.newBuilder()
            .setLineage(LineageEntry.newBuilder()
                .setId(TEST_LINEAGE_ID)
                .addAllInputFiles(Arrays.asList(TEST_FILE_ID))
                .addAllOutputFileIds(Arrays.asList(TEST_FILE_ID))
                .setJobCommand(TEST_JOB_COMMAND)
                .setJobOutputPath(TEST_JOB_OUTPUT_PATH)
                .setCreationTimeMs(TEST_OP_TIME_MS))
            .build())
        .add(
            JournalEntry.newBuilder()
            .setLineageIdGenerator(LineageIdGeneratorEntry.newBuilder()
                .setSequenceNumber(TEST_SEQUENCE_NUMBER))
            .build())
        .add(JournalEntry.newBuilder()
            .setAsyncPersistRequest(AsyncPersistRequestEntry.newBuilder()
                .setFileId(1L))
            .build())
        .add(
            JournalEntry.newBuilder()
            .setSetState(SetStateEntry.newBuilder()
                .setId(TEST_FILE_ID)
                .setOpTimeMs(TEST_OP_TIME_MS)
                .setPinned(true)
                .setPersisted(true)
                .setTtl(TEST_TTL))
            .build())
        .add(
            JournalEntry.newBuilder()
            .setCompletePartition(CompletePartitionEntry.newBuilder()
                .setBlockId(TEST_BLOCK_ID)
                .setKeyLimit(TEST_KEY1)
                .setKeyStart(TEST_KEY2))
            .build())
        .add(
            JournalEntry.newBuilder()
                .setCreateStore(CreateStoreEntry.newBuilder()
                    .setStoreId(TEST_FILE_ID))
                .build())
        .add(
            JournalEntry.newBuilder()
                .setCompleteStore(CompleteStoreEntry.newBuilder()
                    .setStoreId(TEST_FILE_ID))
                .build())
        .build();
    // Add the test sequence number to every journal entry
    ENTRIES_LIST = Lists.transform(entries, new Function<JournalEntry, JournalEntry>() {
      @Override
      public JournalEntry apply(JournalEntry entry) {
        return entry.toBuilder().setSequenceNumber(TEST_SEQUENCE_NUMBER).build();
      }
    });
  }

  /**
   * @return the implementation of {@link JournalFormatter} that wants to be tested
   */
  protected abstract JournalFormatter getFormatter();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    String path = mTestFolder.newFile().getAbsolutePath();
    mOs = new FileOutputStream(path);
    mIs = new FileInputStream(path);
  }

  @After
  public final void after() throws Exception {
    mOs.close();
    mIs.close();
  }

  protected void write(JournalEntry entry) throws IOException {
    mFormatter.serialize(entry, mOs);
  }

  protected JournalEntry read() throws IOException {
    JournalInputStream jis = mFormatter.deserialize(mIs);
    JournalEntry entry = jis.getNextEntry();
    Assert.assertEquals(TEST_SEQUENCE_NUMBER, jis.getLatestSequenceNumber());
    return entry;
  }

  protected void assertSameEntry(JournalEntry entry1, JournalEntry entry2) {
    Assert.assertEquals(entry1, entry2);
  }

  protected void entryTest(JournalEntry entry) throws IOException {
    write(entry);
    JournalEntry readEntry = read();
    assertSameEntry(entry, readEntry);
  }

  // check if every entry is covered by this test
  @Test
  public void checkEntriesNumberTest() {
    // Subtract one to exclude ENTRY_NOT_SET
    Assert.assertEquals(JournalEntry.EntryCase.values().length - 1, ENTRIES_LIST.size());
  }

  @Test
  public void entriesTest() throws IOException {
    for (JournalEntry entry : ENTRIES_LIST) {
      entryTest(entry);
    }
  }
}
