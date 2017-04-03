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
 * Unit tests for {@link UfsJournalCheckpointWriter}.
 */
public final class UfsJournalCheckpointWriterTest {
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
  public void writeJournalEntry() throws Exception {
    long endSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(false).setNextSequenceNumber(endSN));
    for (int i = 0; i < 5; ++i) {
      writer.write(newEntry(i));
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.mCheckpoints.size());
    Assert.assertEquals(expectedCheckpoint, snapshot.mCheckpoints.get(0).getLocation().toString());
    Assert.assertTrue(snapshot.mTemporaryCheckpoints.isEmpty());
  }

  @Test
  public void writeJournalEntryMoreThanJournalLogSequenceNumber() throws Exception {
    long endSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(false).setNextSequenceNumber(endSN));
    for (int i = 0; i < endSN + 10; ++i) {
      writer.write(newEntry(i));
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.mCheckpoints.size());
    Assert.assertEquals(expectedCheckpoint, snapshot.mCheckpoints.get(0).getLocation().toString());
    Assert.assertTrue(snapshot.mTemporaryCheckpoints.isEmpty());
  }

  @Test
  public void cancel() throws Exception {
    long endSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(false).setNextSequenceNumber(endSN));
    for (int i = 0; i < 5; ++i) {
      writer.write(newEntry(i));
    }
    writer.cancel();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertTrue(snapshot.mCheckpoints.isEmpty());
    Assert.assertTrue(snapshot.mTemporaryCheckpoints.isEmpty());
  }

  @Test
  public void checkpointExists() throws Exception {
    long endSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(false).setNextSequenceNumber(endSN));
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    mJournal.getUfs().create(expectedCheckpoint).close();
    for (int i = 0; i < 5; ++i) {
      writer.write(newEntry(i));
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertEquals(1, snapshot.mCheckpoints.size());
    Assert.assertEquals(expectedCheckpoint, snapshot.mCheckpoints.get(0).getLocation().toString());
    Assert.assertTrue(snapshot.mTemporaryCheckpoints.isEmpty());
  }

  @Test
  public void olderCheckpointExists() throws Exception {
    long endSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(false).setNextSequenceNumber(endSN));
    String oldCheckpoint = URIUtils
        .appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN - 1))
        .toString();
    mJournal.getUfs().create(oldCheckpoint).close();
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    for (int i = 0; i < 5; ++i) {
      writer.write(newEntry(i));
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertEquals(2, snapshot.mCheckpoints.size());
    Assert.assertEquals(oldCheckpoint, snapshot.mCheckpoints.get(0).getLocation().toString());
    Assert.assertEquals(expectedCheckpoint, snapshot.mCheckpoints.get(1).getLocation().toString());
    Assert.assertTrue(snapshot.mTemporaryCheckpoints.isEmpty());
  }

  @Test
  public void newerCheckpointExists() throws Exception {
    long endSN = 0x20;
    JournalWriter writer = mJournal.getWriter(
        JournalWriterOptions.defaults().setPrimary(false).setNextSequenceNumber(endSN));
    String newerCheckpoint = URIUtils
        .appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN + 1))
        .toString();
    mJournal.getUfs().create(newerCheckpoint).close();
    for (int i = 0; i < 5; ++i) {
      writer.write(newEntry(i));
    }
    writer.close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertEquals(1, snapshot.mCheckpoints.size());
    Assert.assertEquals(newerCheckpoint, snapshot.mCheckpoints.get(0).getLocation().toString());
    Assert.assertTrue(snapshot.mTemporaryCheckpoints.isEmpty());
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
