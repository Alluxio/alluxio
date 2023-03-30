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

package alluxio.master.journal.raft;

import static alluxio.master.journal.raft.RaftSnapshotManagerTest.createSampleSnapshot;
import static alluxio.master.journal.raft.RaftSnapshotManagerTest.createStateMachineStorage;

import net.bytebuddy.utility.RandomString;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class SnapshotDirStateMachineStorageTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  final SnapshotRetentionPolicy mRetentionPolicy = new SnapshotRetentionPolicy() {
    @Override
    public int getNumSnapshotsRetained() {
      return 1; // keep only 1 snapshot
    }
  };
  SnapshotDirStateMachineStorage mStateMachineStorage;

  @Before
  public void before() throws IOException {
    mStateMachineStorage = createStateMachineStorage(mFolder);
  }

  @Test
  public void noSnapshot() {
    Assert.assertNull(mStateMachineStorage.getLatestSnapshot());
  }

  @Test
  public void onlyUpdateOnLoad() throws IOException {
    Assert.assertNull(mStateMachineStorage.getLatestSnapshot());
    createSampleSnapshot(mStateMachineStorage, 1, 10);
    // still null until new information is loaded
    Assert.assertNull(mStateMachineStorage.getLatestSnapshot());
  }

  @Test
  public void singleSnapshot() throws IOException {
    createSampleSnapshot(mStateMachineStorage, 1, 10);
    mStateMachineStorage.loadLatestSnapshot();
    SnapshotInfo latestSnapshot = mStateMachineStorage.getLatestSnapshot();
    Assert.assertTrue(latestSnapshot instanceof FileListSnapshotInfo);
    Assert.assertEquals(TermIndex.valueOf(1, 10), latestSnapshot.getTermIndex());
  }

  @Test
  public void newerIndex() throws IOException {
    createSampleSnapshot(mStateMachineStorage, 1, 10);
    mStateMachineStorage.loadLatestSnapshot();
    Assert.assertEquals(TermIndex.valueOf(1, 10),
        mStateMachineStorage.getLatestSnapshot().getTermIndex());
    createSampleSnapshot(mStateMachineStorage, 1, 15);
    mStateMachineStorage.loadLatestSnapshot();
    Assert.assertEquals(TermIndex.valueOf(1, 15),
        mStateMachineStorage.getLatestSnapshot().getTermIndex());
  }

  @Test
  public void newerTerm() throws IOException {
    createSampleSnapshot(mStateMachineStorage, 1, 10);
    mStateMachineStorage.loadLatestSnapshot();
    Assert.assertEquals(TermIndex.valueOf(1, 10),
        mStateMachineStorage.getLatestSnapshot().getTermIndex());
    createSampleSnapshot(mStateMachineStorage, 2, 5);
    mStateMachineStorage.loadLatestSnapshot();
    Assert.assertEquals(TermIndex.valueOf(2, 5),
        mStateMachineStorage.getLatestSnapshot().getTermIndex());
  }

  @Test
  public void noDeletionUnlessSignaled() throws IOException {
    createSampleSnapshot(mStateMachineStorage, 1, 1);
    createSampleSnapshot(mStateMachineStorage, 2, 10);
    createSampleSnapshot(mStateMachineStorage, 3, 100);

    mStateMachineStorage.loadLatestSnapshot();
    mStateMachineStorage.cleanupOldSnapshots(mRetentionPolicy);
    // no deletion unless signaled
    try (Stream<Path> s = Files.list(mStateMachineStorage.getSnapshotDir().toPath())) {
      Assert.assertEquals(3, s.count());
    }
  }

  @Test
  public void noopDeleteIfEmpty() throws IOException {
    mStateMachineStorage.loadLatestSnapshot();
    mStateMachineStorage.signalNewSnapshot();
    mStateMachineStorage.cleanupOldSnapshots(mRetentionPolicy);
    try (Stream<Path> s = Files.list(mStateMachineStorage.getSnapshotDir().toPath())) {
      Assert.assertEquals(0, s.count());
    }
  }

  @Test
  public void noopDeleteIfOneOnly() throws IOException {
    createSampleSnapshot(mStateMachineStorage, 1, 10);

    mStateMachineStorage.loadLatestSnapshot();
    mStateMachineStorage.signalNewSnapshot();
    mStateMachineStorage.cleanupOldSnapshots(mRetentionPolicy);
    // no deletion unless signaled
    try (Stream<Path> s = Files.list(mStateMachineStorage.getSnapshotDir().toPath())) {
      Assert.assertEquals(1, s.count());
    }
  }

  @Test
  public void deleteMultiple() throws IOException {
    createSampleSnapshot(mStateMachineStorage, 1, 1);
    createSampleSnapshot(mStateMachineStorage, 2, 10);
    createSampleSnapshot(mStateMachineStorage, 3, 100);

    mStateMachineStorage.signalNewSnapshot();
    mStateMachineStorage.cleanupOldSnapshots(mRetentionPolicy);
    // no deletion unless signaled
    try (Stream<Path> s = Files.list(mStateMachineStorage.getSnapshotDir().toPath())) {
      Assert.assertEquals(1, s.count());
    }
    mStateMachineStorage.loadLatestSnapshot();
    Assert.assertEquals(TermIndex.valueOf(3, 100),
        mStateMachineStorage.getLatestSnapshot().getTermIndex());
  }

  @Test
  public void backwardsCompatible() throws IOException {
    createSampleSnapshot(mStateMachineStorage, 1, 1);
    String snapshotFile = SimpleStateMachineStorage.getSnapshotFileName(2, 10);
    try (FileOutputStream outputStream =
             new FileOutputStream(new File(mStateMachineStorage.getSnapshotDir(), snapshotFile))) {
      outputStream.write(RandomString.make().getBytes());
    }
    mStateMachineStorage.loadLatestSnapshot();
    SnapshotInfo latestSnapshot = mStateMachineStorage.getLatestSnapshot();
    Assert.assertTrue(latestSnapshot instanceof SingleFileSnapshotInfo);
    Assert.assertEquals(TermIndex.valueOf(2, 10), latestSnapshot.getTermIndex());
  }
}
