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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.master.block.journal.BlockContainerIdGeneratorEntry;
import tachyon.master.block.journal.BlockInfoEntry;
import tachyon.master.file.journal.AddMountPointEntry;
import tachyon.master.file.journal.DeleteMountPointEntry;
import tachyon.master.file.journal.CompleteFileEntry;
import tachyon.master.file.journal.DeleteFileEntry;
import tachyon.master.file.journal.DependencyEntry;
import tachyon.master.file.journal.InodeDirectoryEntry;
import tachyon.master.file.journal.InodeDirectoryIdGeneratorEntry;
import tachyon.master.file.journal.InodeFileEntry;
import tachyon.master.file.journal.InodeLastModificationTimeEntry;
import tachyon.master.file.journal.RenameEntry;
import tachyon.master.file.journal.SetPinnedEntry;
import tachyon.master.file.meta.DependencyType;
import tachyon.master.rawtable.journal.RawTableEntry;
import tachyon.util.io.BufferUtils;

/**
 * Base class for testing different {@link JournalFormatter}'s serialization/deserialization
 * correctness of each entry type defined in {@link JournalEntryType}.
 *
 * To test an implementation of {@link JournalFormatter} like {@link JsonJournalFormatter}, extend
 * this class and override method {@link #getFormatter()}.
 *
 * See example usage in {@link JsonJournalFormatterTest}.
 */
public abstract class JournalFormatterTestBase {
  protected static final long TEST_CONTAINER_ID = 2011L;
  protected static final long TEST_BLOCK_ID = 2015L;
  protected static final long TEST_FILE_ID = 1L;
  protected static final String TEST_FILE_NAME = "journalFormatter.test";
  protected static final long TEST_LENGTH_BYTES = 256L;
  protected static final long TEST_BLOCK_SIZE_BYTES = 256L;
  protected static final long TEST_TABLE_ID = 2L;
  protected static final long TEST_OP_TIME_MS = 1409349750338L;
  protected static final long TEST_SEQUENCE_NUMBER = 1945L;
  protected static final TachyonURI TEST_TACHYON_PATH = new TachyonURI("/test/path");
  protected static final TachyonURI TEST_UFS_PATH = new TachyonURI("hdfs://host:port/test/path");

  protected JournalFormatter mFormatter = getFormatter();
  protected OutputStream mOs;
  protected InputStream mIs;

  /**
   * Returns the implementation of {@link JournalFormatter} that wants to be tested.
   */
  protected abstract JournalFormatter getFormatter();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public final void before() throws Exception {
    String path = mTestFolder.newFile().getAbsolutePath();
    mOs = new FileOutputStream(path);
    mIs = new FileInputStream(path);
  }

  protected void write(JournalEntry entry) throws IOException {
    mFormatter.serialize(new SerializableJournalEntry(TEST_SEQUENCE_NUMBER, entry), mOs);
    mOs.close();
  }

  protected JournalEntry read() throws IOException {
    JournalInputStream jis = mFormatter.deserialize(mIs);
    JournalEntry entry = jis.getNextEntry();
    Assert.assertEquals(TEST_SEQUENCE_NUMBER, jis.getLatestSequenceNumber());
    jis.close();
    return entry;
  }

  protected void assertSameEntry(JournalEntry entry1, JournalEntry entry2) {
    Assert.assertTrue(entry1.getParameters().equals(entry2.getParameters()));
  }

  protected void entryTest(JournalEntry entry) throws IOException {
    write(entry);
    JournalEntry readEntry = read();
    assertSameEntry(entry, readEntry);
  }

  // Block

  @Test
  public void blockIdGeneratorEntryTest() throws IOException {
    entryTest(new BlockContainerIdGeneratorEntry(TEST_CONTAINER_ID));
  }

  @Test
  public void blockInfoEntryTest() throws IOException {
    entryTest(new BlockInfoEntry(TEST_BLOCK_ID, TEST_LENGTH_BYTES));
  }

  // FileSystem

  @Test
  public void inodeFileEntryTest() throws IOException {
    List<Long> blocks = new ArrayList<Long>(10);
    for (int i = 0; i < 10; i ++) {
      blocks.add(TEST_BLOCK_ID + i);
    }
    entryTest(new InodeFileEntry(TEST_OP_TIME_MS, TEST_FILE_ID, TEST_FILE_NAME, TEST_FILE_ID, true,
        true, TEST_OP_TIME_MS, TEST_BLOCK_SIZE_BYTES, TEST_LENGTH_BYTES, true, true, blocks,
        Constants.NO_TTL));
  }

  @Test
  public void inodeDirectoryEntryTest() throws IOException {
    Set<Long> childrenIds = new HashSet<Long>(10);
    for (int i = 0; i < 10; i ++) {
      childrenIds.add(TEST_FILE_ID + i);
    }
    entryTest(new InodeDirectoryEntry(TEST_OP_TIME_MS, TEST_FILE_ID, TEST_FILE_NAME, TEST_FILE_ID,
        true, true, TEST_OP_TIME_MS, childrenIds));
  }

  @Test
  public void inodeLastModificationTimeEntryTest() throws IOException {
    entryTest(new InodeLastModificationTimeEntry(TEST_FILE_ID, TEST_OP_TIME_MS));
  }

  @Test
  public void dependencyEntryTest() throws IOException {
    List<Long> parents = Arrays.asList(1L, 2L, 3L);
    List<Long> children = Arrays.asList(4L, 5L, 6L, 7L);
    String commandPrefix = "fake command";
    List<ByteBuffer> data = Arrays.asList(ByteBuffer.wrap(Base64.decodeBase64("AAAAAAAAAAAAA==")));
    String comment = "Comment Test";
    String framework = "Tachyon Examples";
    String frameworkVersion = "0.3";
    DependencyType dependencyType = DependencyType.Narrow;
    List<Integer> parentDepIds = Arrays.asList(1, 2, 3);
    List<Integer> childrenDepIds = Arrays.asList(4, 5, 6, 7);
    List<Long> unCheckpointedFileIds = Arrays.asList(1L, 2L);
    Set<Long> lostFileIds = Sets.newHashSet(4L, 5L, 6L);
    int depId = 1;
    entryTest(new DependencyEntry(depId, parents, children, commandPrefix, data, comment,
        framework, frameworkVersion, dependencyType, parentDepIds, childrenDepIds, TEST_OP_TIME_MS,
        unCheckpointedFileIds, lostFileIds));
  }

  @Test
  public void completeFileEntryTest() throws IOException {
    entryTest(new CompleteFileEntry(Arrays.asList(1L, 2L, 3L), TEST_FILE_ID, TEST_LENGTH_BYTES,
        TEST_OP_TIME_MS));
  }

  @Test
  public void setPinnedEntryTest() throws IOException {
    entryTest(new SetPinnedEntry(TEST_FILE_ID, false, TEST_OP_TIME_MS));
  }

  @Test
  public void deleteFileEntryTest() throws IOException {
    entryTest(new DeleteFileEntry(TEST_FILE_ID, true, TEST_OP_TIME_MS));
  }

  @Test
  public void renameEntryTest() throws IOException {
    entryTest(new RenameEntry(TEST_FILE_ID, TEST_FILE_NAME, TEST_OP_TIME_MS));
  }

  @Test
  public void inodeDirectoryIdGeneratorEntryTest() throws IOException {
    entryTest(new InodeDirectoryIdGeneratorEntry(TEST_CONTAINER_ID, TEST_SEQUENCE_NUMBER));
  }

  @Test
  public void addMountPointEntryTest() throws IOException {
    entryTest(new AddMountPointEntry(TEST_TACHYON_PATH, TEST_UFS_PATH));
  }

  @Test
  public void deleteMountPointEntryTest() throws IOException {
    entryTest(new DeleteMountPointEntry(TEST_TACHYON_PATH));
  }

  // RawTable

  @Test
  public void rawTableEntryTest() throws IOException {
    entryTest(new RawTableEntry(TEST_BLOCK_ID, 100, BufferUtils.getIncreasingByteBuffer(10)));
  }
}
