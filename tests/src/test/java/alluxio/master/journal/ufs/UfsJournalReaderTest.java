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

package alluxio.master.journal.ufs;

import alluxio.BaseIntegrationTest;
import alluxio.ConfigurationTestUtils;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.options.JournalReaderOptions;
import alluxio.master.journal.options.JournalWriterOptions;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.URIUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.net.URI;

/**
 * Unit tests for {@link UfsJournalReader}.
 */
public final class UfsJournalReaderTest extends BaseIntegrationTest {
  private static final long CHECKPOINT_SIZE = 10;
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;
  private UnderFileSystem mUfs;

  @Before
  public void before() throws Exception {
    URI location = URIUtils
        .appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()), "FileSystemMaster");
    mUfs = Mockito.spy(UnderFileSystem.Factory.create(location));
    mJournal = new UfsJournal(location, mUfs);
  }

  @After
  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Reads checkpoint.
   */
  @Test
  public void readCheckpoint() throws Exception {
    long endSN = 0x20;
    buildCheckpoint(endSN);
    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(true))) {
      Journal.JournalEntry entry;
      int sn = 0;
      while ((entry = reader.read()) != null) {
        Assert.assertEquals(sn, entry.getSequenceNumber());
        sn++;
      }

      Assert.assertEquals(CHECKPOINT_SIZE, sn);
      Assert.assertEquals(endSN, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads a checkpoint that has journal log sequence number (encoded in the file) smaller
   * than the number of journal entries in the checkpoint.
   */
  @Test
  public void readCheckpointLogSequenceSmallerThanCheckpointSize() throws Exception {
    long endSN = 0x1;
    buildCheckpoint(endSN);
    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(true))) {
      Journal.JournalEntry entry;
      int sn = 0;
      while ((entry = reader.read()) != null) {
        Assert.assertEquals(sn, entry.getSequenceNumber());
        sn++;
      }

      Assert.assertEquals(CHECKPOINT_SIZE, sn);
      Assert.assertEquals(endSN, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads completed logs.
   */
  @Test
  public void readCompletedLog() throws Exception {
    long fileSize = 10;
    long endSN = 10 * fileSize;
    for (long i = 0; i < endSN / fileSize; i++) {
      buildCompletedLog(i * fileSize, i * fileSize + fileSize);
    }
    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(true))) {
      Journal.JournalEntry entry;
      int sn = 0;
      while ((entry = reader.read()) != null) {
        Assert.assertEquals(sn, entry.getSequenceNumber());
        sn++;
      }

      Assert.assertEquals(endSN, sn);
      Assert.assertEquals(sn, reader.getNextSequenceNumber());

      // Further reads should return null.
      Assert.assertTrue(reader.read() == null);
    }
  }

  /**
   * Reads incomplete logs in a primary master.
   */
  @Test
  public void readIncompleteLogPrimary() throws Exception {
    long endSN = 10;
    buildCompletedLog(0, endSN);
    buildIncompleteLog(endSN, endSN + 1);
    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(true))) {
      Journal.JournalEntry entry;
      int sn = 0;
      while ((entry = reader.read()) != null) {
        Assert.assertEquals(sn, entry.getSequenceNumber());
        sn++;
      }

      Assert.assertEquals(endSN + 1, sn);
      Assert.assertEquals(sn, reader.getNextSequenceNumber());
    }
  }

  /**
   * Secondary master cannot read incomplete logs.
   */
  @Test
  public void readIncompleteLogSecondary() throws Exception {
    long endSN = 10;
    buildCompletedLog(0, endSN);
    buildIncompleteLog(endSN, endSN + 1);
    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(false))) {
      Journal.JournalEntry entry;
      int sn = 0;
      while ((entry = reader.read()) != null) {
        Assert.assertEquals(sn, entry.getSequenceNumber());
        sn++;
      }

      Assert.assertEquals(endSN, sn);
      Assert.assertEquals(sn, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads logs created while reading the journal.
   */
  @Test
  public void readNewLogs() throws Exception {
    long endSN = 10;
    buildCompletedLog(0, endSN);

    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(true))) {
      Journal.JournalEntry entry;
      int sn = 0;
      while ((entry = reader.read()) != null) {
        Assert.assertEquals(sn, entry.getSequenceNumber());
        sn++;
      }

      Assert.assertEquals(endSN, sn);
      Assert.assertEquals(sn, reader.getNextSequenceNumber());

      // Write another two logs.
      buildCompletedLog(endSN, endSN * 2);
      buildIncompleteLog(endSN * 2, endSN * 2 + 1);

      while ((entry = reader.read()) != null) {
        Assert.assertEquals(sn, entry.getSequenceNumber());
        sn++;
      }

      Assert.assertEquals(endSN * 2 + 1, sn);
      Assert.assertEquals(sn, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads checkpoint and logs. The checkpoint's last sequence number does not match any one of
   * the logs' sequence number.
   */
  @Test
  public void readCheckpointAndLogsSnNotMatch() throws Exception {
    long fileSize = 10;
    buildCheckpoint(fileSize * 3 + 1);

    for (int i = 0; i < 10; i++) {
      buildCompletedLog(i * fileSize, (i + 1) * fileSize);
    }

    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(true))) {
      while ((reader.read()) != null) {
      }
      Assert.assertEquals(10 * fileSize, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads checkpoint and logs. The checkpoint's last sequence number matches one of
   * the logs' sequence number.
   */
  @Test
  public void readCheckpointAndLogsSnMatch() throws Exception {
    long fileSize = 10;
    buildCheckpoint(fileSize * 3);

    for (int i = 0; i < 10; i++) {
      buildCompletedLog(i * fileSize, (i + 1) * fileSize);
    }

    try (JournalReader reader = mJournal
        .getReader(JournalReaderOptions.defaults().setPrimary(true))) {
      while ((reader.read()) != null) {
      }
      Assert.assertEquals(10 * fileSize, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads checkpoint and logs from a non-zero sequence number. The given sequence number is within
   * the checkpoint.
   */
  @Test
  public void resumeReadingWithinCheckpoint() throws Exception {
    long fileSize = 10;
    buildCheckpoint(fileSize * 3);

    for (int i = 0; i < 10; i++) {
      buildCompletedLog(i * fileSize, (i + 1) * fileSize);
    }

    try (JournalReader reader = mJournal.getReader(
        JournalReaderOptions.defaults().setPrimary(true).setNextSequenceNumber(fileSize * 2))) {
      while ((reader.read()) != null) {
      }
      Assert.assertEquals(10 * fileSize, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads checkpoint and logs from a non-zero sequence number. The given sequence number is after
   * the checkpoint.
   */
  @Test
  public void resumeReadingAfterCheckpoint() throws Exception {
    long fileSize = 10;
    buildCheckpoint(fileSize * 3);

    for (int i = 0; i < 10; i++) {
      buildCompletedLog(i * fileSize, (i + 1) * fileSize);
    }

    try (JournalReader reader = mJournal.getReader(
        JournalReaderOptions.defaults().setPrimary(true).setNextSequenceNumber(fileSize * 3 + 1))) {
      while ((reader.read()) != null) {
      }
      Assert.assertEquals(10 * fileSize, reader.getNextSequenceNumber());
    }
  }

  /**
   * Builds checkpoint.
   *
   * @param sequenceNumber the sequence number after the checkpoint
   */
  private void buildCheckpoint(long sequenceNumber) throws Exception {
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(false).setNextSequenceNumber(sequenceNumber));
    for (int i = 0; i < CHECKPOINT_SIZE; i++) {
      writer.write(newEntry(i));
    }
    writer.close();
  }

  /**
   * Builds complete log from the sequence number interval.
   *
   * @param start start of the sequence number (included)
   * @param end end of the sequence number (excluded)
   */
  private void buildCompletedLog(long start, long end) throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    JournalWriter writer = mJournal
        .getWriter(JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(start));
    for (long i = start; i < end; i++) {
      writer.write(newEntry(i));
    }
    writer.close();
  }

  /**
   * Builds incomplete log.
   *
   * @param start start of the sequence number (included)
   * @param end end of the sequence number (excluded)
   */
  private void buildIncompleteLog(long start, long end) throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    buildCompletedLog(start, end);
    Assert.assertTrue(
        mUfs.renameFile(UfsJournalFile.encodeLogFileLocation(mJournal, start, end).toString(),
            UfsJournalFile
                .encodeLogFileLocation(mJournal, start, UfsJournal.UNKNOWN_SEQUENCE_NUMBER)
                .toString()));
  }

  /**
   * Creates a dummy journal entry with the given sequence number.
   *
   * @param sequenceNumber the sequence number
   * @return the journal entry
   */
  private Journal.JournalEntry newEntry(long sequenceNumber) {
    return Journal.JournalEntry.newBuilder().setSequenceNumber(sequenceNumber).build();
  }
}
