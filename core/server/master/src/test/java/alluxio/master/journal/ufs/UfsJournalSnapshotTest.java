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

import alluxio.master.NoopMaster;
import alluxio.util.URIUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;

/**
 * Unit tests for {@link UfsJournalSnapshot}.
 */
public final class UfsJournalSnapshotTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;

  @Before
  public void before() throws Exception {
    mJournal =
        new UfsJournal(URIUtils.appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()),
            "FileSystemMaster"), new NoopMaster(), 0, Collections::emptySet);
  }

  /**
   * Tests journal snapshot.
   */
  @Test
  public void snapshot() throws Exception {
    String c1 = UfsJournalFile.encodeCheckpointFileLocation(mJournal, 0x10).toString();
    String c2 = UfsJournalFile.encodeCheckpointFileLocation(mJournal, 0x12).toString();
    mJournal.getUfs().create(c1).close();
    mJournal.getUfs().create(c2).close();

    String t = UfsJournalFile.encodeTemporaryCheckpointFileLocation(mJournal).toString();
    mJournal.getUfs().create(t).close();

    long start = 0x11;
    Queue<String> expectedLogs = new ArrayDeque<>();
    for (int i = 0; i < 10; i++) {
      String l =
          UfsJournalFile.encodeLogFileLocation(mJournal, start + i, start + i + 2).toString();
      expectedLogs.add(l);
      mJournal.getUfs().create(l).close();
      start = start + i + 2;
    }
    String currentLog =
        UfsJournalFile.encodeLogFileLocation(mJournal, start, UfsJournal.UNKNOWN_SEQUENCE_NUMBER)
            .toString();
    expectedLogs.add(currentLog);
    mJournal.getUfs().create(currentLog).close();

    // Write a malformed log file which should be skipped.
    mJournal.getUfs()
        .create(UfsJournalFile.encodeLogFileLocation(mJournal, 0x10, 0x100).toString() + ".tmp")
        .close();

    UfsJournalSnapshot snapshot = UfsJournalSnapshot.getSnapshot(mJournal);
    Assert.assertEquals(c1, snapshot.getCheckpoints().get(0).getLocation().toString());
    Assert.assertEquals(c2, snapshot.getCheckpoints().get(1).getLocation().toString());

    Assert.assertEquals(t, snapshot.getTemporaryCheckpoints().get(0).getLocation().toString());

    Assert.assertEquals(expectedLogs.size(), snapshot.getLogs().size());
    for (UfsJournalFile log : snapshot.getLogs()) {
      Assert.assertEquals(expectedLogs.poll(), log.getLocation().toString());
    }

    Assert.assertEquals(currentLog,
        UfsJournalSnapshot.getCurrentLog(mJournal).getLocation().toString());

    Assert.assertEquals(0x12, UfsJournalSnapshot.getNextLogSequenceNumberToCheckpoint(mJournal));
  }
}
