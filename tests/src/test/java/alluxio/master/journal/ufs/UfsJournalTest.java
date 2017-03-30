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

import alluxio.util.URIUtils;

import org.eclipse.jetty.util.ArrayQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.Queue;

/**
 * Unit tests for {@link UfsJournal}.
 */
public final class UfsJournalTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private UfsJournal mJournal;

  @Before
  public void before() throws Exception {
    mJournal = new UfsJournal(URIUtils
        .appendPathOrDie(new URI(mFolder.newFolder().getAbsolutePath()), "FileSystemMaster"));
  }

  @Test
  public void completedLogFilename() throws Exception {
    String location = mJournal.encodeLogFileLocation(0x10, 0x100).toString();
    Assert.assertEquals(URIUtils.appendPathOrDie(mJournal.getLogDir(), "0x10-0x100").toString(),
        location);
    UfsJournalFile file = mJournal.decodeCheckpointOrLogFile("0x10-0x100", false);
    Assert.assertTrue(file.isCompletedLog());
    Assert.assertEquals(0x10, file.getStart());
    Assert.assertEquals(0x100, file.getEnd());
    Assert.assertEquals(location, file.getLocation().toString());
  }

  @Test
  public void incompleteLogFilename() throws Exception {
    String location =
        mJournal.encodeLogFileLocation(0x10, UfsJournal.UNKNOWN_SEQUENCE_NUMBER).toString();
    String expectedFilename = "0x10-0x" + Long.toHexString(UfsJournal.UNKNOWN_SEQUENCE_NUMBER);
    Assert.assertEquals(URIUtils.appendPathOrDie(mJournal.getLogDir(), expectedFilename).toString(),
        location);
    UfsJournalFile file = mJournal.decodeCheckpointOrLogFile(expectedFilename, false);
    Assert.assertTrue(file.isIncompleteLog());
    Assert.assertEquals(0x10, file.getStart());
    Assert.assertEquals(UfsJournal.UNKNOWN_SEQUENCE_NUMBER, file.getEnd());
    Assert.assertEquals(location, file.getLocation().toString());
  }

  @Test
  public void checkpointFilename() throws Exception {
    String location = mJournal.encodeCheckpointFileLocation(0x10).toString();
    String expectedFilename = "0x0-0x10";
    Assert.assertEquals(
        URIUtils.appendPathOrDie(mJournal.getCheckpointDir(), expectedFilename).toString(),
        location);
    UfsJournalFile file = mJournal.decodeCheckpointOrLogFile(expectedFilename, true);
    Assert.assertTrue(file.isCheckpoint());
    Assert.assertEquals(0x0, file.getStart());
    Assert.assertEquals(0x10, file.getEnd());
    Assert.assertEquals(location, file.getLocation().toString());
  }

  @Test
  public void temporaryCheckpointFilename() throws Exception {
    String location = mJournal.encodeTemporaryCheckpointFileLocation().toString();
    Assert.assertTrue(location.startsWith(mJournal.getTmpDir().toString()));
    UfsJournalFile file =
        mJournal.decodeTemporaryCheckpointFile(location.substring(location.lastIndexOf('/') + 1));
    Assert.assertTrue(file.isTmpCheckpoint());
    Assert.assertEquals(UfsJournal.UNKNOWN_SEQUENCE_NUMBER, file.getStart());
    Assert.assertEquals(UfsJournal.UNKNOWN_SEQUENCE_NUMBER, file.getEnd());
    Assert.assertEquals(location, file.getLocation().toString());
  }

  @Test
  public void snapshot() throws Exception {
    String c1 = mJournal.encodeCheckpointFileLocation(0x10).toString();
    String c2 = mJournal.encodeCheckpointFileLocation(0x12).toString();
    mJournal.getUfs().create(c1).close();
    mJournal.getUfs().create(c2).close();

    String t = mJournal.encodeTemporaryCheckpointFileLocation().toString();
    mJournal.getUfs().create(t).close();

    long start = 0x11;
    Queue<String> expectedLogs = new ArrayQueue<>();
    for (int i = 0; i < 10; ++i) {
      String l = mJournal.encodeLogFileLocation(start + i, start + i + 2).toString();
      expectedLogs.add(l);
      mJournal.getUfs().create(l).close();
      start = start + i + 2;
    }
    String currentLog =
        mJournal.encodeLogFileLocation(start, UfsJournal.UNKNOWN_SEQUENCE_NUMBER).toString();
    expectedLogs.add(currentLog);
    mJournal.getUfs().create(currentLog).close();

    // Write a malformed log file which should be skipped.
    mJournal.getUfs().create(mJournal.encodeLogFileLocation(0x10, 0x100).toString() + ".tmp")
        .close();

    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertEquals(c1, snapshot.mCheckpoints.get(0).getLocation().toString());
    Assert.assertEquals(c2, snapshot.mCheckpoints.get(1).getLocation().toString());

    Assert.assertEquals(t, snapshot.mTemporaryCheckpoints.get(0).getLocation().toString());

    Assert.assertEquals(expectedLogs.size(), snapshot.mLogs.size());
    for (UfsJournalFile log : snapshot.mLogs) {
      Assert.assertEquals(expectedLogs.poll(), log.getLocation().toString());
    }

    Assert.assertEquals(currentLog, mJournal.getCurrentLog().getLocation().toString());

    Assert.assertEquals(0x12, mJournal.getNextLogSequenceToCheckpoint());
  }

  @Test
  public void format() throws Exception {
    mJournal.getUfs().create(mJournal.encodeCheckpointFileLocation(0x12).toString()).close();
    mJournal.getUfs().create(mJournal.encodeTemporaryCheckpointFileLocation().toString()).close();

    long start = 0x11;
    for (int i = 0; i < 10; ++i) {
      String l = mJournal.encodeLogFileLocation(start + i, start + i + 2).toString();
      mJournal.getUfs().create(l).close();
      start = start + i + 2;
    }
    String currentLog =
        mJournal.encodeLogFileLocation(start, UfsJournal.UNKNOWN_SEQUENCE_NUMBER).toString();
    mJournal.getUfs().create(currentLog).close();
    // Write a malformed log file which should be skipped.
    mJournal.getUfs().create(mJournal.encodeLogFileLocation(0x10, 0x100).toString() + ".tmp")
        .close();

    mJournal.format();
    Assert.assertTrue(mJournal.isFormatted());
    UfsJournal.Snapshot snapshot = mJournal.getSnapshot();
    Assert.assertTrue(snapshot.mCheckpoints.isEmpty());
    Assert.assertTrue(snapshot.mLogs.isEmpty());
    Assert.assertTrue(snapshot.mTemporaryCheckpoints.isEmpty());
  }
}
