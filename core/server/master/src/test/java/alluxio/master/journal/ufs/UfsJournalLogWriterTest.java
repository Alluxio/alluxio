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

import static org.hamcrest.CoreMatchers.containsString;

import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.master.journal.JournalReader.State;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.URIUtils;

import org.hamcrest.core.StringStartsWith;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.Collections;

/**
 * Unit tests for {@link UfsJournalLogWriter}.
 */
public final class UfsJournalLogWriterTest {
  private static final String INJECTED_IO_ERROR_MESSAGE = "injected I/O error";

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

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
   * A new journal writer completes the current log.
   */
  @Test
  public void completeCurrentLog() throws Exception {
    long startSN = 0x10;
    long endSN = 0x20;
    mJournal.getUfs().create(
        UfsJournalFile.encodeLogFileLocation(mJournal, startSN, UfsJournal.UNKNOWN_SEQUENCE_NUMBER)
            .toString()).close();
    new UfsJournalLogWriter(mJournal, 0x20).close();
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);

    String expectedLog =
        URIUtils.appendPathOrDie(mJournal.getLogDir(), String.format("0x%x-0x%x", startSN, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.getLogs().size());
    Assert.assertEquals(expectedLog, snapshot.getLogs().get(0).getLocation().toString());
    Assert.assertTrue(UfsJournalSnapshot.getCurrentLog(mJournal) == null);
  }

  /**
   * The completed log corresponds to the current log exists.
   */
  @Test
  public void duplicateCompletedLog() throws Exception {
    long startSN = 0x10;
    long endSN = 0x20;
    mJournal.getUfs().create(
        UfsJournalFile.encodeLogFileLocation(mJournal, startSN, UfsJournal.UNKNOWN_SEQUENCE_NUMBER)
            .toString()).close();
    mJournal.getUfs()
        .create(UfsJournalFile.encodeLogFileLocation(mJournal, startSN, endSN).toString()).close();
    new UfsJournalLogWriter(mJournal, startSN).close();
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);

    String expectedLog =
        URIUtils.appendPathOrDie(mJournal.getLogDir(), String.format("0x%x-0x%x", startSN, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.getLogs().size());
    Assert.assertEquals(expectedLog, snapshot.getLogs().get(0).getLocation().toString());
    Assert.assertTrue(UfsJournalSnapshot.getCurrentLog(mJournal) == null);
  }

  /**
   * Writes journal entries while the UFS has flush supported.
   */
  @Test
  public void writeJournalEntryUfsHasFlush() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    long nextSN = 0x20;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, nextSN);

    for (int i = 0; i < 10; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      if (i % 5 == 0) {
        writer.flush();
      }
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertTrue(snapshot.getCheckpoints().isEmpty());
    // 0x20 - 0x2a
    Assert.assertEquals(1, snapshot.getLogs().size());
    Assert.assertEquals(UfsJournalFile.encodeLogFileLocation(mJournal, 0x20, 0x2a),
        snapshot.getLogs().get(0).getLocation());
  }

  /**
   * Writes journal entries while the UFS has no flush supported.
   */
  @Test
  public void writeJournalEntryUfsNoFlush() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(false);
    long nextSN = 0x20;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, nextSN);
    for (int i = 0; i < 10; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      if (i % 5 == 0) {
        writer.flush();
      }
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertTrue(snapshot.getCheckpoints().isEmpty());
    // 0x20 - 0x21, 0x21 - 0x26, 0x26 - 0x2a
    Assert.assertEquals(3, snapshot.getLogs().size());
    Assert.assertEquals(UfsJournalFile.encodeLogFileLocation(mJournal, 0x20, 0x21),
        snapshot.getLogs().get(0).getLocation());
    Assert.assertEquals(UfsJournalFile.encodeLogFileLocation(mJournal, 0x21, 0x26),
        snapshot.getLogs().get(1).getLocation());
    Assert.assertEquals(UfsJournalFile.encodeLogFileLocation(mJournal, 0x26, 0x2a),
        snapshot.getLogs().get(2).getLocation());
  }

  /**
   * Tests journal rotation.
   */
  @Test
  public void writeJournalEntryRotate() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "1");

    long nextSN = 0x20;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, nextSN);
    for (int i = 0; i < 10; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      writer.flush();
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertTrue(snapshot.getCheckpoints().isEmpty());
    Assert.assertEquals(10, snapshot.getLogs().size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(UfsJournalFile.encodeLogFileLocation(mJournal, 0x20 + i, 0x21 + i),
          snapshot.getLogs().get(i).getLocation());
    }
  }

  /**
   * Tests that (1) {@link UfsJournalLogWriter#mJournalOutputStream} is reset when an exception
   * is thrown, and (2) the {@link UfsJournalLogWriter} recovers during the next write. It should
   * write out all entries, including the one that initially failed.
   */
  @Test
  public void recoverFromUfsFailure() throws Exception {
    long startSN = 0x10;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, startSN);
    final int numberOfEntriesWrittenBeforeFailure = 10;
    long nextSN = writeJournalEntries(writer, startSN, numberOfEntriesWrittenBeforeFailure);
    DataOutputStream badOut = createMockDataOutputStream(writer);
    Mockito.doThrow(new IOException(INJECTED_IO_ERROR_MESSAGE)).when(badOut)
        .write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
    tryWriteAndExpectToFail(writer, nextSN);
    // UfsJournalLogWriter will perform recovery before the write operation.
    writer.write(newEntry(nextSN));
    nextSN++;
    writer.close();

    checkJournalEntries(startSN, nextSN);
  }

  /**
   * Tests that {@link UfsJournalLogWriter#flush()} can recover after Ufs failure.
   *
   * This test has the following steps.
   * 1. Write several journal entries, flush and close the journal. This will create a complete
   *    journal file, i.e. <startSN>-<endSN>.
   * 2. Write another journal entry, do NOT flush it, and inject an I/O error.
   * 3. Attempt to write another journal entry, which is expected to fail. After the failure,
   *    the newly created incomplete journal file may or may not have valid content.
   * 4. The UFS does not guarantee the consistency of the incomplete journal. To
   *    test how {@link UfsJournalLogWriter} recovers from Ufs failure before
   *    {@link UfsJournalLogWriter#flush()}, we simply delete the possibly created incomplete
   *    journal.
   */
  @Test
  public void flushAfterUfsFailure() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);

    // Write several journal entries, creating and closing journal file.
    // This file is complete.
    long startSN = 0x10;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, startSN);
    final int numberOfEntriesWrittenBeforeFailure = 10;
    long nextSN = writeJournalEntries(writer, startSN, numberOfEntriesWrittenBeforeFailure);
    writer.flush();
    writer.close();

    // Write another entry without flushing.
    writer = new UfsJournalLogWriter(mJournal, nextSN);
    nextSN = writeJournalEntries(writer, nextSN, 1);
    DataOutputStream badOut = createMockDataOutputStream(writer);
    Mockito.doThrow(new IOException(INJECTED_IO_ERROR_MESSAGE)).when(badOut)
        .write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
    tryWriteAndExpectToFail(writer, nextSN);

    // Delete the incomplete journal file to simulate the case in which
    // journal entries that have not been explicitly flushed will be lost.
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    UfsJournalFile journalFile = snapshot.getCurrentLog(mJournal);
    File file = new File(journalFile.getLocation().toString());
    file.delete();

    // Flush the journal, recovering from the Ufs failure.
    writer.flush();
    checkJournalEntries(startSN, nextSN);
    writer.close();
  }

  /**
   * Test that failures during {@link UfsJournalLogWriter#flush()} will complete the current file.
   */
  @Test
  public void flushFailureCompletesFile() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);

    // Write several journal entries, creating and closing journal file.
    // This file is complete.
    long startSN = 0x10;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, startSN);
    final int numberOfEntriesWrittenBeforeFailure = 10;
    long nextSN = writeJournalEntries(writer, startSN, numberOfEntriesWrittenBeforeFailure);
    writer.flush();
    writer.close();

    // Write another entry and then fail the flush.
    writer = new UfsJournalLogWriter(mJournal, nextSN);
    nextSN = writeJournalEntries(writer, nextSN, 1);
    DataOutputStream badOut = createMockDataOutputStream(writer);
    Mockito.doThrow(new IOException(INJECTED_IO_ERROR_MESSAGE)).when(badOut).flush();
    nextSN = writeJournalEntries(writer, nextSN, 1);
    tryFlushAndExpectToFail(writer);

    // Retry the flush, expect it to rotate the log and start a new file
    writer.flush();
    // Complete the last log
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertEquals(3, snapshot.getLogs().size());
    Assert.assertEquals(nextSN - 1, snapshot.getLogs().get(2).getStart());
    Assert.assertEquals(nextSN, snapshot.getLogs().get(2).getEnd());
  }

  /**
   * Tests that {@link UfsJournalLogWriter} can detect the failure in which some flushed journal
   * entries are missing from the journal during recovery.
   */
  @Test
  public void missingJournalEntries() throws Exception {
    long startSN = 0x10;
    long nextSN = startSN;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, nextSN);
    long truncateSize = 0;
    long firstCorruptedEntrySeq = startSN + 4;
    for (int i = 0; i < 5; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      if (i == 3) {
        writer.flush();
        UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
        UfsJournalFile journalFile = snapshot.getCurrentLog(mJournal);
        File file = new File(journalFile.getLocation().toString());
        truncateSize = file.length();
      }
    }
    writer.flush();

    writer.write(newEntry(nextSN));
    long seqOfFirstEntryToFlush = nextSN;
    nextSN++;
    Assert.assertNotNull(writer.getJournalOutputStream());
    DataOutputStream badOut = createMockDataOutputStream(writer);
    Mockito.doThrow(new IOException(INJECTED_IO_ERROR_MESSAGE)).when(badOut)
        .write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
    tryWriteAndExpectToFail(writer, nextSN);
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    UfsJournalFile journalFile = snapshot.getCurrentLog(mJournal);
    File file = new File(journalFile.getLocation().toString());
    try (FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        FileChannel fileChannel = fileOutputStream.getChannel()) {
      fileChannel.truncate(truncateSize);
    }
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(
        ExceptionMessage.JOURNAL_ENTRY_MISSING.getMessageWithUrl(
            RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
            firstCorruptedEntrySeq, seqOfFirstEntryToFlush));
    writer.write(newEntry(nextSN));
    writer.close();
  }

  @Test
  public void recoverWithNoJournalFiles() throws Exception {
    long startSN = 0x10;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, startSN);
    // Put stream into a bad state
    long nextSN = writeJournalEntries(writer, startSN, 5);
    DataOutputStream badOut = createMockDataOutputStream(writer);
    Mockito.doThrow(new IOException(INJECTED_IO_ERROR_MESSAGE)).when(badOut).flush();
    tryFlushAndExpectToFail(writer);

    // Delete the file
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    UfsJournalFile journalFile = snapshot.getLogs().get(0);
    File file = new File(journalFile.getLocation().toString());
    file.delete();

    // Recover should fail since we deleted the file
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(
        StringStartsWith.startsWith("Cannot find any journal entry to recover."));
    writer.write(newEntry(nextSN));
    writer.close();
  }

  @Test
  public void recoverMissingJournalFiles() throws Exception {
    long startSN = 0x10;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, startSN);
    // Create a file
    long nextSN = writeJournalEntries(writer, startSN, 5);
    writer.close();

    // Flush some entries to the next file
    writer = new UfsJournalLogWriter(mJournal, nextSN);
    nextSN = writeJournalEntries(writer, nextSN, 5);
    writer.flush();

    // Put the stream into a bad state
    nextSN = writeJournalEntries(writer, nextSN, 5);
    DataOutputStream badOut = createMockDataOutputStream(writer);
    Mockito.doThrow(new IOException(INJECTED_IO_ERROR_MESSAGE)).when(badOut).flush();
    tryFlushAndExpectToFail(writer);

    // Delete the current log
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    UfsJournalFile journalFile = snapshot.getCurrentLog(mJournal);
    File file = new File(journalFile.getLocation().toString());
    file.delete();

    // Recover should fail since we deleted the current log
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(
        ExceptionMessage.JOURNAL_ENTRY_MISSING.getMessageWithUrl(
            RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, 0x15, 0x1A));
    writer.write(newEntry(nextSN));
    writer.close();
  }

  /**
   * Writes {@code numberOfEntries} journal entries starting from {@code startSN}.
   *
   * @param writer {@link UfsJournalLogWriter} instance to use
   * @param startSN start journal sequence number
   * @param numberOfEntries number of journal entries
   * @return next journal entry sequence number after writing these entries
   */
  private long writeJournalEntries(UfsJournalLogWriter writer, long startSN,
      int numberOfEntries) throws Exception {
    long nextSN = startSN;
    for (int i = 0; i < numberOfEntries; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
    }
    Assert.assertNotNull(writer.getJournalOutputStream());
    return nextSN;
  }

  /**
   * Creates a mock {@link DataOutputStream} for {@link UfsJournalLogWriter#mJournalOutputStream}.
   * Mock's internal state is altered to report its written byte count as 1.
   *
   * @param writer the {@link UfsJournalLogWriter} instance for which the mock is created
   * @return the created mock {@link DataOutputStream} instance
   * @throws IOException
   */
  private DataOutputStream createMockDataOutputStream(UfsJournalLogWriter writer)
      throws IOException {
    flushOutputStream(writer);
    DataOutputStream badOut = Mockito.mock(DataOutputStream.class);
    Whitebox.setInternalState(badOut, "written", 1);
    Object journalOutputStream = writer.getJournalOutputStream();
    Whitebox.setInternalState(journalOutputStream, "mOutputStream", badOut);
    return badOut;
  }

  /**
   * Flushes the writer's data output stream.
   * @param writer
   * @throws IOException
   */
  private void flushOutputStream(UfsJournalLogWriter writer) throws IOException {
    Object journalOutputStream = writer.getJournalOutputStream();
    DataOutputStream mOutputStream =
        Whitebox.getInternalState(journalOutputStream, "mOutputStream");
    mOutputStream.flush();
  }

  /**
   * Tries to write a journal entry and expects the write to fail due to {@link IOException}.
   *
   * @param writer {@link UfsJournalLogWriter} that attempts the write
   * @param nextSN the sequence number that the entry is expected to have
   */
  private void tryWriteAndExpectToFail(UfsJournalLogWriter writer, long nextSN) throws Exception {
    try {
      writer.write(newEntry(nextSN));
      Assert.fail("Should not reach here.");
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(), containsString(INJECTED_IO_ERROR_MESSAGE));
    }
  }

  /**
   * Tries to flush a journalwriter it to fail due to {@link IOException}.
   *
   * @param writer {@link UfsJournalLogWriter} that attempts the write
   */
  private void tryFlushAndExpectToFail(UfsJournalLogWriter writer) throws Exception {
    try {
      writer.flush();
      Assert.fail("Should not reach here.");
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(), containsString(INJECTED_IO_ERROR_MESSAGE));
    }
  }

  /**
   * Checks that journal entries with sequence number between startSN (inclusive) and endSN
   * (exclusive) exist in the current journal files.
   *
   * @param startSN start sequence number (inclusive)
   * @param endSN end sequence number (exclusive)
   */
  private void checkJournalEntries(long startSN, long endSN)
      throws IOException, InvalidJournalEntryException {
    try (JournalReader reader = new UfsJournalReader(mJournal, startSN, true)) {
      long seq = startSN;
      while (reader.advance() == State.LOG) {
        Assert.assertEquals(seq, reader.getEntry().getSequenceNumber());
        seq++;
      }
      Assert.assertEquals(endSN, seq);
    }
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
