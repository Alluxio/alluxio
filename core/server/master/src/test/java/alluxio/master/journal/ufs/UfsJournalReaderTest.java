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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.conf.ServerConfiguration;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalReader.State;
import alluxio.master.journal.checkpoint.CheckpointOutputStream;
import alluxio.master.journal.checkpoint.CheckpointType;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Collections;

/**
 * Unit tests for {@link UfsJournalReader}.
 */
public final class UfsJournalReaderTest {
  private static final long CHECKPOINT_SIZE = 10;
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;
  private UnderFileSystem mUfs;

  @Before
  public void before() throws Exception {
    URI location = URIUtils
        .appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()), "FileSystemMaster");
    mUfs = Mockito
        .spy(UnderFileSystem.Factory.create(location.toString(), ServerConfiguration.global()));
    mJournal = new UfsJournal(location, new NoopMaster(), mUfs, 0, Collections::emptySet);
    mJournal.start();
    mJournal.gainPrimacy();
  }

  @After
  public void after() throws Exception {
    mJournal.close();
    ServerConfiguration.reset();
  }

  /**
   * Reads checkpoint.
   */
  @Test
  public void readCheckpoint() throws Exception {
    long endSN = CHECKPOINT_SIZE;
    byte[] checkpointBytes = buildCheckpoint(endSN);
    try (JournalReader reader = mJournal.getReader(true)) {
      State state;
      boolean foundCheckpoint = false;
      while ((state = reader.advance()) != State.DONE) {
        switch (state) {
          case CHECKPOINT:
            foundCheckpoint = true;
            assertArrayEquals(checkpointBytes, IOUtils.toByteArray(reader.getCheckpoint()));
            break;
          case LOG:
          case DONE:
          default:
            throw new IllegalStateException("Unexpected state: " + state);
        }
      }
      assertTrue(foundCheckpoint);
      assertEquals(endSN, reader.getNextSequenceNumber());
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
    try (JournalReader reader = mJournal.getReader(true)) {
      int sn = 0;
      while (reader.advance() != State.DONE) {
        assertEquals(sn, reader.getEntry().getSequenceNumber());
        sn++;
      }

      assertEquals(endSN, sn);
      assertEquals(sn, reader.getNextSequenceNumber());

      // Further advances should return State.DONE
      assertEquals(State.DONE, reader.advance());
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
    try (JournalReader reader = mJournal.getReader(true)) {
      int sn = 0;
      while (reader.advance() != State.DONE) {
        assertEquals(sn, reader.getEntry().getSequenceNumber());
        sn++;
      }

      assertEquals(endSN + 1, sn);
      assertEquals(sn, reader.getNextSequenceNumber());
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
    try (JournalReader reader = mJournal.getReader(false)) {
      int sn = 0;
      while (reader.advance() != State.DONE) {
        assertEquals(sn, reader.getEntry().getSequenceNumber());
        sn++;
      }

      assertEquals(endSN, sn);
      assertEquals(sn, reader.getNextSequenceNumber());
    }
  }

  /**
   * Reads logs created while reading the journal.
   */
  @Test
  public void readNewLogs() throws Exception {
    long endSN = 10;
    buildCompletedLog(0, endSN);

    try (JournalReader reader = mJournal.getReader(true)) {
      int sn = 0;
      while (reader.advance() != State.DONE) {
        assertEquals(sn, reader.getEntry().getSequenceNumber());
        sn++;
      }

      assertEquals(endSN, sn);
      assertEquals(sn, reader.getNextSequenceNumber());

      // Write another two logs.
      buildCompletedLog(endSN, endSN * 2);
      buildIncompleteLog(endSN * 2, endSN * 2 + 1);

      while (reader.advance() != State.DONE) {
        assertEquals(sn, reader.getEntry().getSequenceNumber());
        sn++;
      }

      assertEquals(endSN * 2 + 1, sn);
      assertEquals(sn, reader.getNextSequenceNumber());
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

    try (JournalReader reader = mJournal.getReader(true)) {
      while (reader.advance() != State.DONE) {
      }
      assertEquals(10 * fileSize, reader.getNextSequenceNumber());
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

    try (JournalReader reader = mJournal.getReader(true)) {
      while (reader.advance() != State.DONE) {
      }
      assertEquals(10 * fileSize, reader.getNextSequenceNumber());
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

    try (JournalReader reader = new UfsJournalReader(mJournal, fileSize * 2, true)) {
      while (reader.advance() != State.DONE) {
      }
      assertEquals(10 * fileSize, reader.getNextSequenceNumber());
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

    try (JournalReader reader = new UfsJournalReader(mJournal, fileSize * 3 + 1, true)) {
      while (reader.advance() != State.DONE) {
      }
      assertEquals(10 * fileSize, reader.getNextSequenceNumber());
    }
  }

  /**
   * Builds checkpoint.
   *
   * @param sequenceNumber the sequence number after the checkpoint
   * @return the checkpoint data
   */
  private byte[] buildCheckpoint(long sequenceNumber) throws Exception {
    byte[] bytes = CommonUtils.randomAlphaNumString(10).getBytes();
    try (UfsJournalCheckpointWriter writer =
        UfsJournalCheckpointWriter.create(mJournal, sequenceNumber)) {
      CheckpointOutputStream stream =
          new CheckpointOutputStream(writer, CheckpointType.JOURNAL_ENTRY);
      stream.write(bytes);
    }
    return bytes;
  }

  /**
   * Builds complete log from the sequence number interval.
   *
   * @param start start of the sequence number (included)
   * @param end end of the sequence number (excluded)
   */
  private void buildCompletedLog(long start, long end) throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, start);
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
    assertTrue(
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
