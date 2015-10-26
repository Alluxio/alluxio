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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.job.JobConf;
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
import tachyon.master.file.journal.SetStateEntry;
import tachyon.master.lineage.journal.AsyncCompleteFileEntry;
import tachyon.master.lineage.journal.DeleteLineageEntry;
import tachyon.master.lineage.journal.LineageEntry;
import tachyon.master.lineage.journal.LineageIdGeneratorEntry;
import tachyon.master.lineage.journal.PersistFilesEntry;
import tachyon.master.lineage.journal.RequestFilePersistenceEntry;
import tachyon.master.lineage.meta.LineageFile;
import tachyon.master.rawtable.journal.RawTableEntry;
import tachyon.master.rawtable.journal.UpdateMetadataEntry;
import tachyon.util.io.BufferUtils;

/**
 * Base class for testing different {@link JournalFormatter}'s serialization/deserialization
 * correctness of each entry type defined in {@link JournalEntryType}.
 * <p>
 * To test an implementation of {@link JournalFormatter} like {@link JsonJournalFormatter}, extend
 * this class and override method {@link #getFormatter()}.
 * <p>
 * See example usage in {@link JsonJournalFormatterTest}.
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
  protected static final TachyonURI TEST_UFS_PATH = new TachyonURI("hdfs://host:port/test/path");
  protected static final Job TEST_JOB = new CommandLineJob("Command", new JobConf("/test/path"));

  protected JournalFormatter mFormatter = getFormatter();
  protected OutputStream mOs;
  protected InputStream mIs;

  // map that holds test journal entries
  protected Map<JournalEntryType, JournalEntry> mDataSet =
      ImmutableMap.<JournalEntryType, JournalEntry>builder()
          .put(JournalEntryType.BLOCK_CONTAINER_ID_GENERATOR,
              new BlockContainerIdGeneratorEntry(TEST_CONTAINER_ID))
          .put(JournalEntryType.BLOCK_INFO, new BlockInfoEntry(TEST_BLOCK_ID, TEST_LENGTH_BYTES))
          .put(JournalEntryType.INODE_FILE,
              new InodeFileEntry(TEST_OP_TIME_MS, TEST_FILE_ID, TEST_FILE_NAME, TEST_FILE_ID, true,
                  true, TEST_OP_TIME_MS, TEST_BLOCK_SIZE_BYTES, TEST_LENGTH_BYTES, true, true,
                  ContiguousSet.create(Range.closedOpen(TEST_BLOCK_ID, TEST_BLOCK_ID + 10),
                      DiscreteDomain.longs()).asList(),
                  Constants.NO_TTL))
          .put(JournalEntryType.INODE_DIRECTORY,
              new InodeDirectoryEntry(TEST_OP_TIME_MS, TEST_FILE_ID, TEST_FILE_NAME, TEST_FILE_ID,
                  true, true, TEST_OP_TIME_MS,
                  ContiguousSet.create(Range.closedOpen(1L, 11L), DiscreteDomain.longs())))
      .put(JournalEntryType.INODE_MTIME,
          new InodeLastModificationTimeEntry(TEST_FILE_ID, TEST_OP_TIME_MS))
      .put(JournalEntryType.INODE_PERSISTED, new PersistDirectoryEntry(TEST_FILE_ID, true))
      .put(JournalEntryType.ADD_CHECKPOINT,
          new PersistFileEntry(TEST_FILE_ID, TEST_LENGTH_BYTES, TEST_OP_TIME_MS))
      .put(JournalEntryType.COMPLETE_FILE,
          new CompleteFileEntry(Arrays.asList(1L, 2L, 3L), TEST_FILE_ID, TEST_LENGTH_BYTES,
              TEST_OP_TIME_MS))
      .put(JournalEntryType.DELETE_FILE, new DeleteFileEntry(TEST_FILE_ID, true, TEST_OP_TIME_MS))
      .put(JournalEntryType.RENAME, new RenameEntry(TEST_FILE_ID, TEST_FILE_NAME, TEST_OP_TIME_MS))
      .put(JournalEntryType.INODE_DIRECTORY_ID_GENERATOR,
          new InodeDirectoryIdGeneratorEntry(TEST_CONTAINER_ID, TEST_SEQUENCE_NUMBER))
      .put(JournalEntryType.ADD_MOUNTPOINT,
          new AddMountPointEntry(TEST_TACHYON_PATH, TEST_UFS_PATH))
      .put(JournalEntryType.DELETE_MOUNTPOINT, new DeleteMountPointEntry(TEST_TACHYON_PATH))
      .put(JournalEntryType.RAW_TABLE,
          new RawTableEntry(TEST_BLOCK_ID, 100, BufferUtils.getIncreasingByteBuffer(10)))
      .put(JournalEntryType.UPDATE_METADATA,
          new UpdateMetadataEntry(TEST_BLOCK_ID, ByteBuffer.wrap(new byte[10])))
      .put(JournalEntryType.REINITIALIZE_FILE,
          new ReinitializeFileEntry(TEST_FILE_NAME, TEST_BLOCK_SIZE_BYTES, TEST_OP_TIME_MS))
      .put(JournalEntryType.ASYNC_COMPLETE_FILE, new AsyncCompleteFileEntry(TEST_FILE_ID))
      .put(JournalEntryType.DELETE_LINEAGE, new DeleteLineageEntry(TEST_LINEAGE_ID, false))
      .put(JournalEntryType.LINEAGE,
          new LineageEntry(TEST_LINEAGE_ID, Collections.<TachyonFile>emptyList(),
              Collections.<LineageFile>emptyList(), TEST_JOB, TEST_OP_TIME_MS))
      .put(JournalEntryType.LINEAGE_ID_GENERATOR, new LineageIdGeneratorEntry(TEST_LINEAGE_ID))
      .put(JournalEntryType.PERSIST_FILES, new PersistFilesEntry(Arrays.asList(1L, 2L)))
      .put(JournalEntryType.REQUEST_FILE_PERSISTENCE,
          new RequestFilePersistenceEntry(Arrays.asList(1L, 2L)))
      .put(JournalEntryType.SET_STATE, new SetStateEntry(TEST_FILE_ID, TEST_OP_TIME_MS, true, null))
      .build();

  /**
   * Returns the implementation of {@link JournalFormatter} that wants to be tested.
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
    mFormatter.serialize(new SerializableJournalEntry(TEST_SEQUENCE_NUMBER, entry), mOs);
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
    Assert.assertEquals(JournalEntryType.values().length, mDataSet.size());
  }

  @Test
  public void entriesTest() throws IOException {
    for (Map.Entry<JournalEntryType, JournalEntry> entry : mDataSet.entrySet()) {
      entryTest(entry.getValue());
    }
  }

}
