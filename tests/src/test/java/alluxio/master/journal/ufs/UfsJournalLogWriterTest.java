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
import alluxio.PropertyKey;
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
public final class UfsJournalLogWriterTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;
  private UnderFileSystem mUfs;

  @Before
  public void before() throws Exception {
    URI location = URIUtils
        .appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()), "FileSystemMaster");
    mUfs = Mockito.spy(UnderFileSystem.Factory.get(location.toString()));
    mJournal = new UfsJournal(location, mUfs);
  }

  @After
  public void after() throws Exception {
    Configuration.defaultInit();
  }

  @Test
  public void completeCurrentLog() throws Exception {
    long startSN = 0x10;
    long endSN = 0x20;
    mJournal.getUfs().create(
        mJournal.encodeLogFileLocation(startSN, UfsJournal.UNKNOWN_SEQUENCE_NUMBER).toString())
        .close();
    mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(endSN))
        .close();
    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();

    String expectedLog =
        URIUtils.appendPathOrDie(mJournal.getLogDir(), String.format("0x%x-0x%x", startSN, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.mLogs.size());
    Assert.assertEquals(expectedLog, snapshot.mLogs.get(0).getLocation().toString());
    Assert.assertTrue(mJournal.getCurrentLog() == null);
  }

  @Test
  public void duplicateCompletedLog() throws Exception {
    long startSN = 0x10;
    long endSN = 0x20;
    mJournal.getUfs().create(
        mJournal.encodeLogFileLocation(startSN, UfsJournal.UNKNOWN_SEQUENCE_NUMBER).toString())
        .close();
    mJournal.getUfs().create(mJournal.encodeLogFileLocation(startSN, endSN).toString()).close();
    mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(endSN))
        .close();
    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();

    String expectedLog =
        URIUtils.appendPathOrDie(mJournal.getLogDir(), String.format("0x%x-0x%x", startSN, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.mLogs.size());
    Assert.assertEquals(expectedLog, snapshot.mLogs.get(0).getLocation().toString());
    Assert.assertTrue(mJournal.getCurrentLog() == null);
  }

  @Test
  public void writeJournalEntryUfsHasFlush() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    long nextSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(nextSN));
    for (int i = 0; i < 10; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      if (i % 5 == 0) {
        writer.flush();
      }
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertTrue(snapshot.mCheckpoints.isEmpty());
    // 0x20 - 0x2a
    Assert.assertEquals(1, snapshot.mLogs.size());
    Assert.assertEquals(mJournal.encodeLogFileLocation(0x20, 0x2a),
        snapshot.mLogs.get(0).getLocation());
  }

  @Test
  public void writeJournalEntryUfsNoFlush() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(false);
    long nextSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(nextSN));
    for (int i = 0; i < 10; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      if (i % 5 == 0) {
        writer.flush();
      }
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertTrue(snapshot.mCheckpoints.isEmpty());
    // 0x20 - 0x21, 0x21 - 0x26, 0x26 - 0x2a
    Assert.assertEquals(3, snapshot.mLogs.size());
    Assert.assertEquals(mJournal.encodeLogFileLocation(0x20, 0x21),
        snapshot.mLogs.get(0).getLocation());
    Assert.assertEquals(mJournal.encodeLogFileLocation(0x21, 0x26),
        snapshot.mLogs.get(1).getLocation());
    Assert.assertEquals(mJournal.encodeLogFileLocation(0x26, 0x2a),
        snapshot.mLogs.get(2).getLocation());
  }

  @Test
  public void writeJournalEntryRotate() throws Exception {
    Mockito.when(mUfs.supportsFlush()).thenReturn(true);
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "1");

    long nextSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(true).setNextSequenceNumber(nextSN));
    for (int i = 0; i < 10; i++) {
      writer.write(newEntry(nextSN));
      nextSN++;
      writer.flush();
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertTrue(snapshot.mCheckpoints.isEmpty());
    Assert.assertEquals(10, snapshot.mLogs.size());
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(mJournal.encodeLogFileLocation(0x20 + i, 0x21 + i),
          snapshot.mLogs.get(i).getLocation());
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
