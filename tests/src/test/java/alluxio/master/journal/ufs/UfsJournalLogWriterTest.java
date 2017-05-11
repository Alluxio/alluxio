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

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.master.journal.JournalWriter;
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
 * Unit tests for {@link UfsJournalLogWriter}.
 */
public final class UfsJournalLogWriterTest extends BaseIntegrationTest {
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
   * A new journal writer completes the current log.
   */
  @Test
  public void completeCurrentLog() throws Exception {
    long startSN = 0x10;
    long endSN = 0x20;
    mJournal.getUfs().create(
        UfsJournalFile.encodeLogFileLocation(mJournal, startSN, UfsJournal.UNKNOWN_SEQUENCE_NUMBER)
            .toString()).close();
    mJournal
        .getWriter(JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(endSN))
        .close();
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
    mJournal
        .getWriter(JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(endSN))
        .close();
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
    JournalWriter writer = mJournal
        .getWriter(JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(nextSN));
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
    JournalWriter writer = mJournal
        .getWriter(JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(nextSN));
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
    JournalWriter writer = mJournal
        .getWriter(JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(nextSN));
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
   * Creates a dummy journal entry with the given sequence number.
   *
   * @param sequenceNumber the sequence number
   * @return the journal entry
   */
  private Journal.JournalEntry newEntry(long sequenceNumber) {
    return Journal.JournalEntry.newBuilder().setSequenceNumber(sequenceNumber).build();
  }
}
