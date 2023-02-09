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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.CountingNoopFileSystemMaster;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URI;
import java.util.Collections;

/**
 * Unit tests for {@link UfsJournal}.
 */
public final class UfsJournalTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private UfsJournal mJournal;
  private CountingNoopFileSystemMaster mFileSystemMaster;

  @Before
  public void before() throws Exception {
    mFileSystemMaster = new CountingNoopFileSystemMaster();
    mJournal =
        new UfsJournal(URIUtils.appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()),
            "FileSystemMaster"), mFileSystemMaster, 0, Collections::emptySet);
  }

  /**
   * Tests formatting journal.
   */
  @Test
  public void format() throws Exception {
    mJournal.getUfs().create(UfsJournalFile.encodeCheckpointFileLocation(mJournal, 0x12).toString())
        .close();
    mJournal.getUfs()
        .create(UfsJournalFile.encodeTemporaryCheckpointFileLocation(mJournal).toString()).close();

    long start = 0x11;
    for (int i = 0; i < 10; i++) {
      String l =
          UfsJournalFile.encodeLogFileLocation(mJournal, start + i, start + i + 2).toString();
      mJournal.getUfs().create(l).close();
      start = start + i + 2;
    }
    String currentLog =
        UfsJournalFile.encodeLogFileLocation(mJournal, start, UfsJournal.UNKNOWN_SEQUENCE_NUMBER)
            .toString();
    mJournal.getUfs().create(currentLog).close();
    // Write a malformed log file which should be skipped.
    mJournal.getUfs()
        .create(UfsJournalFile.encodeLogFileLocation(mJournal, 0x10, 0x100).toString() + ".tmp")
        .close();

    mJournal.format();
    Assert.assertTrue(mJournal.isFormatted());
    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertTrue(snapshot.getCheckpoints().isEmpty());
    Assert.assertTrue(snapshot.getLogs().isEmpty());
    Assert.assertTrue(snapshot.getTemporaryCheckpoints().isEmpty());
  }

  @Test
  public void unavailableAfterClose() throws Exception {
    mJournal.start();
    mJournal.close();
    mThrown.expect(UnavailableException.class);
    mJournal.createJournalContext();
  }

  @Test
  public void suspendNotAllowedOnPrimary() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();
    mThrown.expect(IllegalStateException.class);
    mJournal.suspend();
  }

  @Test
  public void suspendCatchupResume() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    // Find journal base path for standby journal to consume the same journal files.
    String parentPath = new File(mJournal.getLocation().getPath()).getParent();
    UfsJournal standbyJournal =
        new UfsJournal(new URI(parentPath), countingMaster, 0, Collections::emptySet);
    standbyJournal.start();

    // Bring current next sequence to 3.
    mJournal.write(Journal.JournalEntry.getDefaultInstance()); // seq-0
    mJournal.write(Journal.JournalEntry.getDefaultInstance()); // seq-1
    mJournal.write(Journal.JournalEntry.getDefaultInstance()); // seq-2
    mJournal.flush();
    // Standby still hasn't seen the updates since the current journal file is not complete.
    standbyJournal.suspend();
    // Write more entries while standby is at backup mode.
    mJournal.write(Journal.JournalEntry.getDefaultInstance()); // seq-3
    mJournal.write(Journal.JournalEntry.getDefaultInstance()); // seq-4
    mJournal.flush();
    // Resume until sequence-1.
    standbyJournal.catchup(1).waitTermination();
    // Entries still reside in an incomplete journal file.
    // After backup, we should have read only up to sequence-1.
    Assert.assertEquals(2, countingMaster.getApplyCount());
    // Initiate primary journal shutdown.
    // Current journal file will be completed and standby should still be suspended.
    mJournal.close();
    Assert.assertEquals(2, countingMaster.getApplyCount());
    // standby should apply new entries after resumed.
    standbyJournal.resume();
    CommonUtils.waitFor("catching up to current state", () -> countingMaster.getApplyCount() == 5);
  }

  @Test
  public void journalInitialReplay() throws Exception {
    Assert.assertEquals(UfsJournalCheckpointThread.CatchupState.NOT_STARTED,
        mJournal.getCatchupState());
    mJournal.start();
    CommonUtils.waitFor("catchup done", () -> mJournal.getCatchupState()
            == UfsJournalCheckpointThread.CatchupState.DONE,
        WaitForOptions.defaults().setTimeoutMs(6000));
    mJournal.gainPrimacy();
    Assert.assertEquals(UfsJournalCheckpointThread.CatchupState.NOT_STARTED,
        mJournal.getCatchupState());
    // Write entries and close to guarantee it's a complete journal file
    int entryCount = 10;
    for (int i = 0; i < entryCount; i++) {
      mJournal.write(Journal.JournalEntry.getDefaultInstance());
    }
    mJournal.flush();
    mJournal.close();

    // Validate the initial replay finished and applied all entries
    // Create a new Journal with the same journal path
    String parentPath = new File(mJournal.getLocation().getPath()).getParent();
    mJournal =
        new UfsJournal(new URI(parentPath), mFileSystemMaster, 0, Collections::emptySet);
    mJournal.start();
    CommonUtils.waitFor("catchup done", () -> mJournal.getCatchupState()
            == UfsJournalCheckpointThread.CatchupState.DONE,
        WaitForOptions.defaults().setTimeoutMs(6000));
    Assert.assertEquals(entryCount, mFileSystemMaster.getApplyCount());
  }

  @Test
  public void journalStandbyCatchup() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();
    UfsJournalLogWriter writer = new UfsJournalLogWriter(mJournal, 0);
    int entryCount = 10;
    for (long i = 0; i < 10; i++) {
      writer.write(Journal.JournalEntry.newBuilder().setSequenceNumber(i).build());
    }
    writer.close();
    mJournal.signalLosePrimacy();
    Assert.assertEquals(0, mFileSystemMaster.getApplyCount());
    mJournal.awaitLosePrimacy();
    // When master steps down, it should start catching up
    CommonUtils.waitFor("catchup done", () -> mJournal.getCatchupState()
            == UfsJournalCheckpointThread.CatchupState.DONE,
        WaitForOptions.defaults().setTimeoutMs(6000));
    // check if logs are applied
    Assert.assertEquals(entryCount, mFileSystemMaster.getApplyCount());
  }

  @Test
  public void gainPrimacyAfterSuspend() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    // Find journal base path for standby journal to consume the same journal files.
    String parentPath = new File(mJournal.getLocation().getPath()).getParent();
    UfsJournal standbyJournal =
        new UfsJournal(new URI(parentPath), countingMaster, 0, Collections::emptySet);
    standbyJournal.start();

    // Suspend standby journal
    standbyJournal.suspend();

    // Write entries
    int entryCount = 10;
    for (int i = 0; i < entryCount; i++) {
      mJournal.write(Journal.JournalEntry.getDefaultInstance());
    }
    mJournal.flush();
    // Validate standby didn't apply any entries yet.
    Assert.assertEquals(0, countingMaster.getApplyCount());

    // Gain primacy.
    standbyJournal.gainPrimacy();
    CommonUtils.waitFor("catching up to current state",
        () -> countingMaster.getApplyCount() == entryCount);

    // Resume should fail after becoming primary.
    mThrown.expect(IllegalStateException.class);
    standbyJournal.resume();
  }

  @Test
  public void subsequentCatchups() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    // Find journal base path for standby journal to consume the same journal files.
    String parentPath = new File(mJournal.getLocation().getPath()).getParent();
    UfsJournal standbyJournal =
        new UfsJournal(new URI(parentPath), countingMaster, 0, Collections::emptySet);
    standbyJournal.start();

    // Suspend standby journal.
    standbyJournal.suspend();

    // Write 2 batches of entries.
    int entryBatchCount = 5;
    for (int i = 0; i < 2 * entryBatchCount; i++) {
      mJournal.write(Journal.JournalEntry.getDefaultInstance());
    }
    mJournal.flush();
    // Validate standby didn't apply any entries yet.
    Assert.assertEquals(0, countingMaster.getApplyCount());

    // Catch up follower journal system to first batch of entries.
    standbyJournal.catchup(entryBatchCount - 1).waitTermination();
    Assert.assertEquals(entryBatchCount, countingMaster.getApplyCount());
    // Catch up follower journal system to second batch of entries.
    standbyJournal.catchup((2 * entryBatchCount) - 1).waitTermination();
    Assert.assertEquals(2 * entryBatchCount, countingMaster.getApplyCount());
  }

  @Test
  public void gainPrimacyDuringCatchup() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = CountingNoopFileSystemMaster.withApplyDelay(50);
    // Find journal base path for standby journal to consume the same journal files.
    String parentPath = new File(mJournal.getLocation().getPath()).getParent();
    UfsJournal standbyJournal =
        new UfsJournal(new URI(parentPath), countingMaster, 0, Collections::emptySet);
    standbyJournal.start();

    // Suspend standby journal.
    standbyJournal.suspend();

    // Write many entries to guarantee that advancing will be in progress
    // when gainPrimacy() is called.
    int entryCount = 10;
    for (int i = 0; i < entryCount; i++) {
      mJournal.write(Journal.JournalEntry.getDefaultInstance());
    }
    mJournal.flush();
    // Validate standby didn't apply any entries yet.
    Assert.assertEquals(0, countingMaster.getApplyCount());

    // Initiate catching up.
    standbyJournal.catchup(entryCount - 2);

    // Gain primacy.
    standbyJournal.gainPrimacy();
    CommonUtils.waitFor("catching up to current state",
        () -> countingMaster.getApplyCount() == entryCount);
  }

  @Test
  public void gainPrimacyAfterCatchup() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    // Find journal base path for standby journal to consume the same journal files.
    String parentPath = new File(mJournal.getLocation().getPath()).getParent();
    UfsJournal standbyJournal =
        new UfsJournal(new URI(parentPath), countingMaster, 0, Collections::emptySet);
    standbyJournal.start();

    // Suspend standby journal.
    standbyJournal.suspend();

    // Write many entries to guarantee that advancing will be in progress
    // when gainPrimacy() is called.
    int entryCount = 10;
    for (int i = 0; i < entryCount; i++) {
      mJournal.write(Journal.JournalEntry.getDefaultInstance());
    }
    mJournal.flush();
    // Validate standby didn't apply any entries yet.
    Assert.assertEquals(0, countingMaster.getApplyCount());

    // Initiate and wait for catching up.
    standbyJournal.catchup(entryCount - 2).waitTermination();
    Assert.assertEquals(entryCount - 1, countingMaster.getApplyCount());

    // Gain primacy.
    standbyJournal.gainPrimacy();
    CommonUtils.waitFor("catching up to current state",
        () -> countingMaster.getApplyCount() == entryCount);
  }

  @Test
  public void catchupCorruptedEntry() throws Exception {
    mJournal.start();
    mJournal.gainPrimacy();

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    // Find journal base path for standby journal to consume the same journal files.
    String parentPath = new File(mJournal.getLocation().getPath()).getParent();
    UfsJournal standbyJournal =
        new UfsJournal(new URI(parentPath), countingMaster, 0, Collections::emptySet);
    standbyJournal.start();

    // Suspend standby journal.
    standbyJournal.suspend();

    int entryCount = 10;
    for (int i = 0; i < entryCount; i++) {
      mJournal.write(Journal.JournalEntry.getDefaultInstance());
    }
    // This one will corrupt the journal catch thread
    Journal.JournalEntry corruptedEntry = Journal.JournalEntry.newBuilder()
        .setSequenceNumber(entryCount + 1)
        .setDeleteFile(alluxio.proto.journal.File.DeleteFileEntry.newBuilder()
            .setId(4563728)
            .setPath("/crash")
            .build())
          .build();
    mJournal.write(corruptedEntry);
    mJournal.flush();

    // The standby catchup thread will crash and the exception will be
    // recovered on waitTermination()
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      CatchupFuture future = standbyJournal.catchup(entryCount);
      future.waitTermination();
    });
    assertTrue(exception.getMessage()
        .contains(CountingNoopFileSystemMaster.ENTRY_DOES_NOT_EXIST));
  }
}
