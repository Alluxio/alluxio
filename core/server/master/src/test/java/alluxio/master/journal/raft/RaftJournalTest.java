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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.master.NoopMaster;
import alluxio.master.StateLockManager;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.CountingNoopFileSystemMaster;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class RaftJournalTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  private RaftJournalSystem mLeaderJournalSystem;
  private RaftJournalSystem mFollowerJournalSystem;

  // A 30sec wait-options object for use by the test.
  private final WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(30_000);

  @Before
  public void before() throws Exception {
    // Create and start journal systems.
    List<RaftJournalSystem> journalSystems = startJournalCluster(createJournalSystems(2));
    // Sleep for 2 leader election cycles for leadership to stabilize.
    Thread.sleep(2
            * Configuration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT));

    // Assign references for leader/follower journal systems.
    mLeaderJournalSystem = journalSystems.get(0);
    mFollowerJournalSystem = journalSystems.get(1);
    CommonUtils.waitFor("a leader is elected",
        () -> mFollowerJournalSystem.isLeader() || mLeaderJournalSystem.isLeader(), mWaitOptions);
    if (journalSystems.get(1).isLeader()) {
      mLeaderJournalSystem = journalSystems.get(1);
      mFollowerJournalSystem = journalSystems.get(0);
    }
    // Transition primary journal to primacy state.
    mLeaderJournalSystem.gainPrimacy();
  }

  @After
  public void after() throws Exception {
    mLeaderJournalSystem.stop();
    mFollowerJournalSystem.stop();
  }

  @Test
  public void writeJournal() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Create entries on the leader journal context.
    // These will be replicated to follower journal context.
    final int entryCount = 10;
    try (JournalContext journalContext =
             mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    // Wait for sequences to be caught up.
    CommonUtils.waitFor("full state acquired", () -> countingMaster.getApplyCount() == entryCount,
        mWaitOptions);
  }

  @Test
  public void joinCluster() throws Exception {
    // Create entries on the leader journal context.
    // These will be replicated to follower journal context.
    final int entryCount = 10;
    try (JournalContext journalContext =
             mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    RaftJournalSystem newJs = createNewJournalSystem(mLeaderJournalSystem);
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    newJs.createJournal(countingMaster);
    newJs.start();

    // Write more entries and validate they are replicated to follower.
    try (JournalContext journalContext =
             mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      journalContext
          .append(alluxio.proto.journal.Journal.JournalEntry.newBuilder()
              .setInodeLastModificationTime(
                  File.InodeLastModificationTimeEntry.newBuilder().setId(entryCount).build())
              .build());
    }
    CommonUtils.waitFor("follower catches up on all changes",
        () -> countingMaster.getApplyCount() == entryCount + 1, mWaitOptions);
  }

  @Test
  public void suspendCatchupResume() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);
    try {
      mFollowerJournalSystem.suspend(null);
      Assert.fail("Suspend succeeded for already suspended journal.");
    } catch (Exception e) {
      // Expected to fail when suspending a suspended journal.
    }

    // Catch up follower journal system to target-index:5.
    final long catchupIndex = 5;
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", catchupIndex);
    CatchupFuture catchupFuture = mFollowerJournalSystem.catchup(backupSequences);

    // Create entries on the leader journal context.
    // These will be replicated to follower journal context.
    final int entryCount = 10;
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    // Wait for sequences to be caught up.
    catchupFuture.waitTermination();
    Assert.assertEquals(catchupIndex + 1, countingMaster.getApplyCount());
    // Wait for election timeout and verify follower master state hasn't changed.
    Thread.sleep(
            Configuration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT));
    Assert.assertEquals(catchupIndex + 1, countingMaster.getApplyCount());
    // Exit backup mode and wait until follower master acquires the current knowledge.
    mFollowerJournalSystem.resume();
    CommonUtils.waitFor("full state acquired", () -> countingMaster.getApplyCount() == entryCount,
        mWaitOptions);

    // Write more entries and validate they are replicated to follower.
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      journalContext
          .append(alluxio.proto.journal.Journal.JournalEntry.newBuilder()
              .setInodeLastModificationTime(
                  File.InodeLastModificationTimeEntry.newBuilder().setId(entryCount).build())
              .build());
    }
    CommonUtils.waitFor("full state acquired after resume",
        () -> countingMaster.getApplyCount() == entryCount + 1, mWaitOptions);
  }

  @Test
  public void suspendSnapshotRestart() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    final int entryCount = 10;
    try (JournalContext journalContext =
             mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);

    // Write more entries which are not applied due to suspension.
    try (JournalContext journalContext =
             mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      journalContext
          .append(alluxio.proto.journal.Journal.JournalEntry.newBuilder()
              .setInodeLastModificationTime(
                  File.InodeLastModificationTimeEntry.newBuilder().setId(entryCount).build())
              .build());
    }

    // Ask the follower to do a snapshot.
    mFollowerJournalSystem.checkpoint(new StateLockManager());

    // Restart the follower.
    mFollowerJournalSystem.stop();
    mFollowerJournalSystem.start();
    Thread.sleep(Configuration.getMs(
        PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT));

    // Verify that all entries are replayed despite the snapshot was requested while some entries
    // are queued up during suspension.
    CommonUtils.waitFor("full state acquired after restart",
        () -> countingMaster.getApplyCount() == entryCount + 1, mWaitOptions);
  }

  // Raft journal receives leader knowledge in chunks.
  // So advancing should take into account seeing partial knowledge.
  @Test
  public void catchUpInSteps() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);

    final int entryBatchCount = 5;
    // Create batch of entries on the leader journal context.
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryBatchCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    // Catch up follower journal system to target-index:(fileCount * 2) - 1.
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", (long) (entryBatchCount * 2) - 1);
    CatchupFuture catchupFuture = mFollowerJournalSystem.catchup(backupSequences);

    // Create next batch of entries on the leader journal context.
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryBatchCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    // Wait for sequence to be caught up.
    catchupFuture.waitTermination();
    Assert.assertEquals(entryBatchCount * 2, countingMaster.getApplyCount());

    // Catchup on the already met sequence.
    mFollowerJournalSystem.catchup(backupSequences);
    Assert.assertEquals(entryBatchCount * 2, countingMaster.getApplyCount());
  }

  @Test
  public void subsequentCatchups() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);

    final int entryBatchCount = 5;
    // Create 2 batches of entries on the leader journal context.
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryBatchCount * 2; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    Map<String, Long> backupSequences = new HashMap<>();
    // Catch up follower journal system to first batch of entries.
    backupSequences.put("FileSystemMaster", (long) entryBatchCount - 1);
    mFollowerJournalSystem.catchup(backupSequences).waitTermination();
    // Catch up follower journal system to second batch of entries.
    backupSequences.put("FileSystemMaster", (long) (2 * entryBatchCount) - 1);
    mFollowerJournalSystem.catchup(backupSequences).waitTermination();

    // Verify master has caught up after advancing.
    Assert.assertEquals(entryBatchCount * 2, countingMaster.getApplyCount());
  }

  @Test
  public void gainPrimacyAfterSuspend() throws Exception {

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);

    // Create entries on the leader journal context.
    // These will be replicated to follower journal context.
    final int entryCount = 10;
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }
    // Assert that no entries applied by suspended journal system.
    Assert.assertEquals(0, countingMaster.getApplyCount());
    // Gain primacy in follower journal and validate it catches up.
    promoteFollower();
    CommonUtils.waitFor(
        "full state acquired after resume", () -> mFollowerJournalSystem.getCurrentSequenceNumbers()
            .values().stream().distinct().collect(Collectors.toList()).get(0) == entryCount - 1,
        mWaitOptions);
    // Follower should no longer be suspended after becoming primary.
    Assert.assertFalse(mFollowerJournalSystem.isSuspended());
  }

  @Test
  public void gainPrimacyAfterCatchup() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);
    // Catch up follower journal system to target-index:5.
    final long catchupIndex = 5;
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", catchupIndex);
    CatchupFuture catchupFuture = mFollowerJournalSystem.catchup(backupSequences);

    // Create entries on the leader journal context.
    // These will be replicated to follower journal context.
    final int entryCount = 10;
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    // Wait until caught up.
    catchupFuture.waitTermination();

    Assert.assertEquals(catchupIndex + 1, countingMaster.getApplyCount());
    // Gain primacy in follower journal and validate it catches up.
    promoteFollower();
    CommonUtils.waitFor("full state acquired after resume",
        () -> countingMaster.getApplyCount() == entryCount, mWaitOptions);

    // Follower should no longer be suspended after becoming primary.
    Assert.assertFalse(mFollowerJournalSystem.isSuspended());
  }

  private void promoteFollower() throws Exception {
    Assert.assertTrue(mLeaderJournalSystem.isLeader());
    Assert.assertFalse(mFollowerJournalSystem.isLeader());
    // Triggering rigged election via reflection to switch the leader.
    NetAddress followerAddress =
        mLeaderJournalSystem.getQuorumServerInfoList().stream()
            .filter(info -> !info.getIsLeader()).findFirst()
            .map(QuorumServerInfo::getServerAddress).get();
    mLeaderJournalSystem.transferLeadership(followerAddress);
    CommonUtils.waitFor("follower becomes leader", () -> mFollowerJournalSystem.isLeader(),
        mWaitOptions);
    Assert.assertFalse(mLeaderJournalSystem.isLeader());
    Assert.assertTrue(mFollowerJournalSystem.isLeader());
    mFollowerJournalSystem.gainPrimacy();
  }

  @Test
  public void gainPrimacyDuringCatchup() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Using a large entry count for catching transition while in-progress.
    final int entryCount = 100;

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);

    // Create entries on the leader journal context.
    // These will be replicated to follower journal context once resumed.
    ForkJoinPool.commonPool().submit(() -> {
      try (JournalContext journalContext =
          mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
        for (int i = 0; i < entryCount; i++) {
          journalContext.append(
              alluxio.proto.journal.Journal.JournalEntry.newBuilder()
                  .setInodeLastModificationTime(
                      File.InodeLastModificationTimeEntry.newBuilder().setId(i).build())
                  .build());
        }
      } catch (Exception e) {
        Assert.fail(String.format("Failed while writing entries: %s", e));
      }
    }).get();
    // Catch up follower journal to a large index to be able to transition while in progress.
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", (long) entryCount);
    // Set delay for each internal processing of entries before initiating catch-up.
    countingMaster.setApplyDelay(100);
    mFollowerJournalSystem.catchup(backupSequences);

    // Wait until advancing starts.
    CommonUtils.waitFor("Advancing to start.", () -> countingMaster.getApplyCount() > 0,
        mWaitOptions);

    // Gain primacy in follower journal
    promoteFollower();
    // Validate it catches up.
    CommonUtils.waitFor("Old olf follower to catch up.",
        () -> countingMaster.getApplyCount() == entryCount, mWaitOptions);

    // Follower should no longer be suspended after becoming primary.
    Assert.assertFalse(mFollowerJournalSystem.isSuspended());
  }

  @Test
  public void catchupCorruptedEntry() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingNoopFileSystemMaster countingMaster = new CountingNoopFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);
    final int entryCount = 3;
    // Suspend follower journal system.
    mFollowerJournalSystem.suspend(null);

    // Create entries on the leader journal context.
    // These will be replicated to follower journal context once resumed.
    ForkJoinPool.commonPool().submit(() -> {
      try (JournalContext journalContext =
           mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
        for (int i = 0; i < entryCount; i++) {
          journalContext.append(
              alluxio.proto.journal.Journal.JournalEntry.newBuilder()
                  .setInodeLastModificationTime(
                      File.InodeLastModificationTimeEntry.newBuilder().setId(i).build())
                  .build());
        }
        // This one will corrupt the journal catch thread
        Journal.JournalEntry corruptedEntry = Journal.JournalEntry
            .newBuilder()
            .setSequenceNumber(4)
            .setDeleteFile(File.DeleteFileEntry.newBuilder()
                .setId(4563728)
                .setPath("/crash")
                .build())
            .build();
        journalContext.append(corruptedEntry);
      } catch (Exception e) {
        Assert.fail(String.format("Failed while writing entries: %s", e));
      }
    }).get();
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", (long) 4);
    // Set delay for each internal processing of entries before initiating catch-up.
    countingMaster.setApplyDelay(1);
    RuntimeException exception = Assert.assertThrows(RuntimeException.class, () -> {
      CatchupFuture future = mFollowerJournalSystem.catchup(backupSequences);
      future.waitTermination();
    });

    Assert.assertTrue(exception.getMessage()
        .contains(CountingNoopFileSystemMaster.ENTRY_DOES_NOT_EXIST));
  }

  @Test
  public void testMergeAlluxioConfig() {
    RaftProperties properties = new RaftProperties();
    try {
      Configuration.set(PropertyKey.fromString(
          PropertyKey.MASTER_EMBEDDED_JOURNAL_RATIS_CONFIG.getName()
              + ".raft.server.rpc.request.timeout"), 123456);
      mLeaderJournalSystem.mergeAlluxioRatisConfig(properties);
      Assert.assertEquals(123456,
          RaftServerConfigKeys.Rpc.requestTimeout(properties).getDuration());
    } finally {
      Configuration.unset(PropertyKey.fromString(
          PropertyKey.MASTER_EMBEDDED_JOURNAL_RATIS_CONFIG.getName()
              + ".raft.server.rpc.request.timeout"));
    }
  }

  /**
   * Creates list of raft journal systems in a clustered mode.
   */
  private List<RaftJournalSystem> createJournalSystems(int journalSystemCount) throws Exception {
    // Override defaults for faster quorum formation.
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, 550);
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, 1100);
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 10);

    List<InetSocketAddress> clusterAddresses = new ArrayList<>(journalSystemCount);
    List<Integer> freePorts = getFreePorts(journalSystemCount);
    for (int i = 0; i < journalSystemCount; i++) {
      clusterAddresses.add(InetSocketAddress.createUnresolved("localhost", freePorts.get(i)));
    }

    List<RaftJournalSystem> journalSystems = new ArrayList<>(journalSystemCount);
    for (int i = 0; i < journalSystemCount; i++) {
      journalSystems.add(new RaftJournalSystem(mFolder.newFolder().toURI(), clusterAddresses.get(i),
           clusterAddresses));
    }
    return journalSystems;
  }

  /**
   * Creates list of raft journal systems in a clustered mode.
   */
  private RaftJournalSystem createNewJournalSystem(RaftJournalSystem seed) throws Exception {
    List<InetSocketAddress> clusterAddresses = seed.getQuorumServerInfoList().stream()
        .map(QuorumServerInfo::getServerAddress)
        .map(address -> InetSocketAddress.createUnresolved(address.getHost(), address.getRpcPort()))
        .collect(Collectors.toList());

    List<Integer> freePorts = getFreePorts(1);
    InetSocketAddress joinAddr = InetSocketAddress.createUnresolved("localhost", freePorts.get(0));
    clusterAddresses.add(joinAddr);
    return new RaftJournalSystem(mFolder.newFolder().toURI(), joinAddr, clusterAddresses);
  }

  /**
   * Starts given journal systems asynchronously and waits until complete.
   */
  private List<RaftJournalSystem> startJournalCluster(List<RaftJournalSystem> journalSystems)
      throws Exception {
    List<CompletableFuture<?>> futures = new LinkedList<>();
    for (RaftJournalSystem js : journalSystems) {
      futures.add(CompletableFuture.runAsync(() -> {
        try {
          js.start();
        } catch (Exception e) {
          throw new RuntimeException("Failed to start journal system.", e);
        }
      }));
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    return journalSystems;
  }

  /**
   * @return a list of free ports
   */
  private List<Integer> getFreePorts(int portCount) throws Exception {
    List<ServerSocket> sockets = new ArrayList<>(portCount);
    for (int i = 0; i < portCount; i++) {
      sockets.add(new ServerSocket(0));
    }
    List<Integer> ports = new ArrayList<>(portCount);
    for (ServerSocket socket : sockets) {
      ports.add(socket.getLocalPort());
      socket.close();
    }
    return ports;
  }
}
