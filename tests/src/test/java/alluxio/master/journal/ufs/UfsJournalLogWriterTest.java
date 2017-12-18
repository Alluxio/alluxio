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

import alluxio.BaseIntegrationTest;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidJournalEntryException;
import alluxio.master.NoopMaster;
import alluxio.master.journal.JournalReader;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.URIUtils;

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

/**
 * Unit tests for {@link UfsJournalLogWriter}.
 */
public final class UfsJournalLogWriterTest extends BaseIntegrationTest {
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
    mUfs = Mockito.spy(UnderFileSystem.Factory.create(location));
    mJournal = new UfsJournal(location, new NoopMaster(), mUfs, 0);
  }

  @After
  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
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
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "1");

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
    long nextSN = startSN;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, startSN);
    final int numberOfEntriesWrittenBeforeFailure = 10;
    for (int i = 0; i < numberOfEntriesWrittenBeforeFailure; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
    }
    Assert.assertNotNull(writer.getJournalOutputStream());
    // Create a mock DataOutputStream.
    DataOutputStream badOut = createMockDataOutputStream(writer);
    // Specify the behavior of badOut so that it throws an IOException containing
    // "injected I/O error" when its `write` method is invoked with any arguments
    // matching (byte[], int, int).
    Mockito.doThrow(new IOException("injected I/O error")).when(badOut)
        .write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
    tryWriteAndExpectToFail(writer, nextSN);
    // Write another entry. UfsJournalLogWriter will perform recovery first.
    writer.write(newEntry(nextSN));
    nextSN++;
    writer.close();

    checkJournalEntries(startSN, nextSN);
  }

  /**
   * Test the case in which there are missing journal entries between the last persisted entry
   * and the first entry in {@code UfsJournalLogWriter#mEntriesToFlush}.
   * {@code UfsJournalLogWriter} should be able to detect this issue.
   */
  @Test
  public void missingJournalEntries() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    long startSN = 0x10;
    long nextSN = startSN;
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, nextSN);
    UfsJournalSnapshot snapshot;
    UfsJournalFile journalFile;
    long truncateSize = 0;
    long firstCorruptedEntrySeq = startSN + 4;
    for (int i = 0; i < 5; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      if (i == 3) {
        snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
        journalFile = snapshot.getCurrentLog(mJournal);
        File file = new File(journalFile.getLocation().toString());
        truncateSize = file.length();
      }
    }
    writer.flush();

    writer.write(newEntry(nextSN));
    long seqOfFirstEntryToFlush = nextSN;
    nextSN++;
    Assert.assertNotNull(writer.getJournalOutputStream());
    // Create a mock DataOutputStream.
    DataOutputStream badOut = createMockDataOutputStream(writer);
    // Specify the behavior of badOut so that it throws an IOException containing
    // "injected I/O error" when its `write` method is invoked with any arguments
    // matching (byte[], int, int).
    Mockito.doThrow(new IOException("injected I/O error")).when(badOut)
        .write(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
    tryWriteAndExpectToFail(writer, nextSN);
    snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    journalFile = snapshot.getCurrentLog(mJournal);
    File file = new File(journalFile.getLocation().toString());
    try (FileOutputStream fileOutputStream = new FileOutputStream(file, true);
        FileChannel fileChannel = fileOutputStream.getChannel()) {
      fileChannel.truncate(truncateSize);
    }
    mThrown.expect(IOException.class);
    mThrown.expectMessage(
        ExceptionMessage.JOURNAL_ENTRY_MISSING.getMessageWithUrl(
            RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL,
            firstCorruptedEntrySeq, seqOfFirstEntryToFlush));
    writer.write(newEntry(nextSN));
    writer.close();
  }

  /**
   * Creates a mock {@link DataOutputStream} for {@link UfsJournalLogWriter#mJournalOutputStream}.
   *
   * @param writer the {@link UfsJournalLogWriter} instance for which the mock is created
   * @return the created mock {@link DataOutputStream} instance
   */
  private DataOutputStream createMockDataOutputStream(UfsJournalLogWriter writer) {
    DataOutputStream badOut = Mockito.mock(DataOutputStream.class);
    Object journalOutputStream = writer.getJournalOutputStream();
    // Use Whitebox to set the private member of journalOutputStream to badOut.
    Whitebox.setInternalState(journalOutputStream, "mOutputStream", badOut);
    Assert.assertEquals(writer.getUnderlyingDataOutputStream(), badOut);
    return badOut;
  }

  /**
   * Tries to write a journal entry and expects the write to fail due to
   * {@link IOException} thrown by {@link DataOutputStream#write(byte[], int, int)}.
   *
   * @param writer {@link UfsJournalLogWriter} that attempts the write
   * @param nextSN the sequence number that the entry is expected to have
   */
  private void tryWriteAndExpectToFail(UfsJournalLogWriter writer, long nextSN) {
    try {
      writer.write(newEntry(nextSN));
      Assert.fail("Should not reach here.");
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(), containsString("injected I/O error"));
    }
  }

  /**
   * Checks that journal entries with sequence number between startSN (inclusive) and endSN
   * (exclusive) exist in the current incomplete journal file, i.e. journal file whose name
   * is in the form of <startSN>-0x7fffffffffffffff.
   *
   * @param startSN start sequence number (inclusive)
   * @param endSN end sequence number (exclusive)
   */
  private void checkJournalEntries(long startSN, long endSN)
      throws IOException, InvalidJournalEntryException {
    try (JournalReader reader = new UfsJournalReader(mJournal, startSN, true)) {
      Journal.JournalEntry entry;
      long expectedSeq = startSN;
      while ((entry = reader.read()) != null) {
        Assert.assertEquals(expectedSeq, entry.getSequenceNumber());
        expectedSeq++;
      }
      Assert.assertEquals(expectedSeq, endSN);
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
