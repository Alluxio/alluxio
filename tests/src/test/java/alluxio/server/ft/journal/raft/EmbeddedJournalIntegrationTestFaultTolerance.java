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

package alluxio.server.ft.journal.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.apache.commons.io.FileUtils;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class EmbeddedJournalIntegrationTestFaultTolerance
    extends EmbeddedJournalIntegrationTestBase {

  private static final int RESTART_TIMEOUT_MS = 6 * Constants.MINUTE_MS;
  private static final int NUM_MASTERS = 3;
  private static final int NUM_WORKERS = 0;

  @Test
  public void failover() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_FAILOVER)
        .setClusterName("EmbeddedJournalFaultTolerance_failover")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    mCluster.waitForAndKillPrimaryMaster(MASTER_INDEX_WAIT_TIME);
    assertTrue(fs.exists(testDir));
    mCluster.notifySuccess();
  }

  @Test
  public void copySnapshotToMaster() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_SNAPSHOT_MASTER)
        .setClusterName("EmbeddedJournalFaultTolerance_copySnapshotToMaster")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 1000)
        .addProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "50KB")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "3s")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "6s")
        .addProperty(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "5s")
        .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    for (int i = 0; i < 2000; i++) {
      fs.createDirectory(testDir.join("file" + i));
    }
    int primaryMasterIndex = mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME);
    String leaderJournalPath = mCluster.getJournalDir(primaryMasterIndex);
    File raftDir = new File(RaftJournalUtils.getRaftJournalDir(new File(leaderJournalPath)),
        RaftJournalSystem.RAFT_GROUP_UUID.toString());
    waitForSnapshot(raftDir);
    mCluster.stopMasters();

    SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    storage.init(new RaftStorageImpl(raftDir,
        RaftServerConfigKeys.Log.CorruptionPolicy.getDefault(),
        RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT.getSize()));
    SingleFileSnapshotInfo snapshot = storage.findLatestSnapshot();
    assertNotNull(snapshot);
    mCluster.notifySuccess();
  }

  @Test
  public void copySnapshotToFollower() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_SNAPSHOT_FOLLOWER)
        .setClusterName("EmbeddedJournalFaultTolerance_copySnapshotToFollower")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 1000)
        .addProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "50KB")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "3s")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "6s")
        .addProperty(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "5s")
        .build();
    mCluster.start();

    int catchUpMasterIndex = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1)
        % NUM_MASTERS;

    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    for (int i = 0; i < 2000; i++) {
      fs.createDirectory(testDir.join("file" + i));
    }
    mCluster.getMetaMasterClient().checkpoint();
    mCluster.stopMaster(catchUpMasterIndex);
    File catchupJournalDir = new File(mCluster.getJournalDir(catchUpMasterIndex));
    FileUtils.deleteDirectory(catchupJournalDir);
    assertTrue(catchupJournalDir.mkdirs());
    mCluster.startMaster(catchUpMasterIndex);
    File raftDir = new File(RaftJournalUtils.getRaftJournalDir(catchupJournalDir),
        RaftJournalSystem.RAFT_GROUP_UUID.toString());
    waitForSnapshot(raftDir);
    mCluster.stopMaster(catchUpMasterIndex);
    SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    storage.init(new RaftStorageImpl(raftDir,
        RaftServerConfigKeys.Log.CorruptionPolicy.getDefault(),
        RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT.getSize()));
    SingleFileSnapshotInfo snapshot = storage.findLatestSnapshot();
    assertNotNull(snapshot);
    mCluster.notifySuccess();
  }

  private void waitForSnapshot(File raftDir) throws InterruptedException, TimeoutException {
    File snapshotDir = new File(raftDir, "sm");
    final int RETRY_INTERVAL_MS = 200; // milliseconds
    CommonUtils.waitFor("snapshot is downloaded", () -> {
      File[] files = snapshotDir.listFiles();
      return files != null && files.length > 1 && files[0].length() > 0;
    }, WaitForOptions.defaults().setInterval(RETRY_INTERVAL_MS).setTimeoutMs(RESTART_TIMEOUT_MS));
  }

  @Test
  public void snapshotTransferLoad() throws Exception {
    int numFile = 500;
    int snapshotPeriod = 50;

    mCluster =
        MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_SNAPSHOT_TRANSFER_LOAD)
        .setClusterName("EmbeddedJournalTransferLeadership_snapshotTransferLoad")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .addProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, snapshotPeriod)
        .build();
    mCluster.start();

    // this operation creates more that numFiles log entries
    for (int i = 0; i < numFile; i++) {
      mCluster.getFileSystemClient().createFile(new AlluxioURI(String.format("/%d", i)));
    }

    // only the latest 3 snapshots are kept, but each snapshot leaves behind a small md5 file.
    // this checks to make sure there are enough md5 files, meaning many snapshots were propagated.
    for (int i = 0; i < NUM_MASTERS; i++) {
      File journalDir = new File(mCluster.getJournalDir(i));
      Path raftDir = Paths.get(RaftJournalUtils.getRaftJournalDir(journalDir).toString(),
          RaftJournalSystem.RAFT_GROUP_UUID.toString());
      try (Stream<Path> stream = Files.walk(raftDir, Integer.MAX_VALUE)) {
        long count = stream.filter(path -> path.toString().endsWith(".md5")).count();
        long expected = numFile / snapshotPeriod * 3 / 2;
        Assert.assertTrue(String.format("Expected at least %d snapshots, got %d", expected,
            count), count >= expected);
      }
    }
    mCluster.notifySuccess();
  }

  @Test
  public void singleMasterSnapshotPurgeLogFiles() throws Exception {
    mCluster =
        MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_SNAPSHOT_SINGLE_MASTER)
            .setClusterName("EmbeddedJournalTransferLeadership_singleMasterSnapshot")
            .setNumMasters(1)
            .setNumWorkers(NUM_WORKERS)
            .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
            .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
            // very small log file size for test purposes
            .addProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "1KB")
            .build();
    mCluster.start();
    mCluster.waitForAllNodesRegistered(5_000);
    File journalDir = new File(mCluster.getJournalDir(0));
    Path raftDir = Paths.get(RaftJournalUtils.getRaftJournalDir(journalDir).toString(),
        RaftJournalSystem.RAFT_GROUP_UUID.toString());
    expectSnapshots(raftDir, 0);
    expectLogFiles(raftDir, 1);
    // create two files so that it closes the first log file and creates a new one
    mCluster.getFileSystemClient().createFile(new AlluxioURI("/testfile0"));
    mCluster.getFileSystemClient().createFile(new AlluxioURI("/testfile1"));
    expectSnapshots(raftDir, 0);
    expectLogFiles(raftDir, 2);
    // take checkpoint aka snapshot, should purge log files
    mCluster.getMetaMasterClient().checkpoint();
    expectSnapshots(raftDir, 1);
    expectLogFiles(raftDir, 1);
    mCluster.notifySuccess();
  }

  private void expectSnapshots(Path raftDir, int numExpected) throws Exception {
    try (Stream<Path> stream = Files.walk(raftDir, Integer.MAX_VALUE)) {
      long countSnapshots = stream.filter(path -> path.toString().endsWith(".md5")).count();
      assertEquals("Expected " + numExpected +  " snapshot(s) to be taken", numExpected,
          countSnapshots);
    }
  }

  private void expectLogFiles(Path raftDir, int numExpected) throws Exception {
    try (Stream<Path> stream = Files.walk(raftDir, Integer.MAX_VALUE)) {
      long countLogFiles =
          stream.filter(path -> path.getFileName().toString().startsWith("log_")).count();
      assertEquals("Expected " + numExpected +  " log file(s)", numExpected, countLogFiles);
    }
  }

  @Test
  public void restart() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_RESTART)
        .setClusterName("EmbeddedJournalFaultTolerance_restart")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .build();
    mCluster.start();

    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    restartMasters();
    assertTrue(fs.exists(testDir));
    restartMasters();
    assertTrue(fs.exists(testDir));
    restartMasters();
    assertTrue(fs.exists(testDir));
    mCluster.notifySuccess();
  }

  @Test
  public void restartStress() throws Throwable {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.EMBEDDED_JOURNAL_RESTART_STRESS)
        .setClusterName("EmbeddedJournalFaultTolerance_restartStress")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
        .build();
    mCluster.start();

    // Run and verify operations while restarting the cluster multiple times.
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger successes = new AtomicInteger(0);
    FileSystem fs = mCluster.getFileSystemClient();
    List<EmbeddedJournalIntegrationTestFaultTolerance.OperationThread> threads = new ArrayList<>();
    try {
      for (int i = 0; i < 10; i++) {
        EmbeddedJournalIntegrationTestFaultTolerance.OperationThread t =
            new EmbeddedJournalIntegrationTestFaultTolerance
                .OperationThread(fs, i, failure, successes);
        t.start();
        threads.add(t);
      }
      for (int i = 0; i < 2; i++) {
        restartMasters();
        successes.set(0);
        CommonUtils.waitFor("11 successes", () -> successes.get() >= 11,
            WaitForOptions.defaults().setTimeoutMs(RESTART_TIMEOUT_MS));
        if (failure.get() != null) {
          throw failure.get();
        }
      }
    } finally {
      threads.forEach(Thread::interrupt);
      for (Thread t : threads) {
        t.join();
      }
    }
    mCluster.notifySuccess();
  }

  private void restartMasters() throws Exception {
    for (int i = 0; i < NUM_MASTERS; i++) {
      mCluster.stopMaster(i);
    }
    for (int i = 0; i < NUM_MASTERS; i++) {
      mCluster.startMaster(i);
    }
  }

  private static class OperationThread extends Thread {
    private final FileSystem mFs;
    private final int mThreadNum;
    private final AtomicReference<Throwable> mFailure;
    private final AtomicInteger mSuccessCounter;

    public OperationThread(FileSystem fs, int threadNum, AtomicReference<Throwable> failure,
        AtomicInteger successCounter) {
      super("operation-test-thread-" + threadNum);
      mFs = fs;
      mThreadNum = threadNum;
      mFailure = failure;
      mSuccessCounter = successCounter;
    }

    public void run() {
      try {
        runInternal();
      } catch (Exception e) {
        e.printStackTrace();
        mFailure.set(e);
      }
    }

    public void runInternal() throws Exception {
      while (!Thread.interrupted()) {
        // 1000 takes over 10 minutes, which exceeds maximum time for a JUnit test
        final int NUM_DIRS = 300;
        for (int i = 0; i < NUM_DIRS; i++) {
          AlluxioURI dir = formatDirName(i);
          try {
            mFs.createDirectory(dir);
          } catch (FileAlreadyExistsException e) {
            // This could happen if the operation was retried but actually succeeded server-side on
            // the first attempt. Alluxio does not de-duplicate retried operations.
            continue;
          }
          if (!mFs.exists(dir)) {
            mFailure.set(new RuntimeException(String.format("Directory %s does not exist", dir)));
            return;
          }
        }
        for (int i = 0; i < NUM_DIRS; i++) {
          AlluxioURI dir = formatDirName(i);
          try {
            mFs.delete(dir);
          } catch (FileDoesNotExistException e) {
            // This could happen if the operation was retried but actually succeeded server-side on
            // the first attempt. Alluxio does not de-duplicate retried operations.
            continue;
          }
          if (mFs.exists(dir)) {
            mFailure.set(new RuntimeException(String.format("Directory %s still exists", dir)));
            return;
          }
        }
        mSuccessCounter.incrementAndGet();
      }
    }

    private AlluxioURI formatDirName(int dirNum) {
      return new AlluxioURI(String.format("/dir-%d-%d", mThreadNum, dirNum));
    }
  }
}
