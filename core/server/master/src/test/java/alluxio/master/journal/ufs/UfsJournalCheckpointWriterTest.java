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

import alluxio.conf.ServerConfiguration;
import alluxio.master.NoopMaster;
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
import java.util.Collections;

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
    mUfs = Mockito
        .spy(UnderFileSystem.Factory.create(location.toString(), ServerConfiguration.global()));
    mJournal = new UfsJournal(location, new NoopMaster(), mUfs, 0, Collections::emptySet);
  }

  @After
  public void after() throws Exception {
    ServerConfiguration.reset();
  }

  /**
   * Writes journal entries to the checkpoint.
   */
  @Test
  public void writeJournalEntry() throws Exception {
    long endSN = 0x20;
    UfsJournalCheckpointWriter writer = mJournal.getCheckpointWriter(endSN);
    for (int i = 0; i < 5; i++) {
      newEntry(i).writeDelimitedTo(writer);
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.getCheckpoints().size());
    Assert.assertEquals(expectedCheckpoint,
        snapshot.getCheckpoints().get(0).getLocation().toString());
    Assert.assertTrue(snapshot.getTemporaryCheckpoints().isEmpty());
  }

  /**
   * The number of journal entries written to the checkpoint is greater than the checkpoint
   * sequence number encoded in the checkpoint file. This is to show that the journal entry's
   * sequence number in the checkpoint file is not relevant to the sequence number encoded in
   * the checkpoint file.
   */
  @Test
  public void writeJournalEntryMoreThanJournalLogSequenceNumber() throws Exception {
    long endSN = 0x20;
    UfsJournalCheckpointWriter writer = mJournal.getCheckpointWriter(endSN);
    for (int i = 0; i < endSN + 10; i++) {
      newEntry(i).writeDelimitedTo(writer);
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    Assert.assertEquals(1, snapshot.getCheckpoints().size());
    Assert.assertEquals(expectedCheckpoint,
        snapshot.getCheckpoints().get(0).getLocation().toString());
    Assert.assertTrue(snapshot.getTemporaryCheckpoints().isEmpty());
  }

  /**
   * Tests cancel.
   */
  @Test
  public void cancel() throws Exception {
    long endSN = 0x20;
    UfsJournalCheckpointWriter writer = mJournal.getCheckpointWriter(endSN);
    for (int i = 0; i < 5; i++) {
      newEntry(i).writeDelimitedTo(writer);
    }
    writer.cancel();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertTrue(snapshot.getCheckpoints().isEmpty());
    Assert.assertTrue(snapshot.getTemporaryCheckpoints().isEmpty());
  }

  /**
   * Tests an existing checkpoint file.
   */
  @Test
  public void checkpointExists() throws Exception {
    long endSN = 0x20;
    UfsJournalCheckpointWriter writer = mJournal.getCheckpointWriter(endSN);
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    mJournal.getUfs().create(expectedCheckpoint).close();
    for (int i = 0; i < 5; i++) {
      newEntry(i).writeDelimitedTo(writer);
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertEquals(1, snapshot.getCheckpoints().size());
    Assert.assertEquals(expectedCheckpoint,
        snapshot.getCheckpoints().get(0).getLocation().toString());
    Assert.assertTrue(snapshot.getTemporaryCheckpoints().isEmpty());
  }

  /**
   * An older checkpoint file exists.
   */
  @Test
  public void olderCheckpointExists() throws Exception {
    long endSN = 0x20;
    UfsJournalCheckpointWriter writer = mJournal.getCheckpointWriter(endSN);
    String oldCheckpoint = URIUtils
        .appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN - 1))
        .toString();
    mJournal.getUfs().create(oldCheckpoint).close();
    String expectedCheckpoint =
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN))
            .toString();
    for (int i = 0; i < 5; i++) {
      newEntry(i).writeDelimitedTo(writer);
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertEquals(2, snapshot.getCheckpoints().size());
    Assert.assertEquals(oldCheckpoint, snapshot.getCheckpoints().get(0).getLocation().toString());
    Assert.assertEquals(expectedCheckpoint,
        snapshot.getCheckpoints().get(1).getLocation().toString());
    Assert.assertTrue(snapshot.getTemporaryCheckpoints().isEmpty());
  }

  /**
   * A newer checkpoint file exists.
   */
  @Test
  public void newerCheckpointExists() throws Exception {
    long endSN = 0x20;
    UfsJournalCheckpointWriter writer = mJournal.getCheckpointWriter(endSN);
    String newerCheckpoint = URIUtils
        .appendPathOrDie(mJournal.getCheckpointDir(), String.format("0x%x-0x%x", 0, endSN + 1))
        .toString();
    mJournal.getUfs().create(newerCheckpoint).close();
    for (int i = 0; i < 5; i++) {
      newEntry(i).writeDelimitedTo(writer);
    }
    writer.close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertEquals(1, snapshot.getCheckpoints().size());
    Assert.assertEquals(newerCheckpoint, snapshot.getCheckpoints().get(0).getLocation().toString());
    Assert.assertTrue(snapshot.getTemporaryCheckpoints().isEmpty());
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
