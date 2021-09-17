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

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.QuorumServerInfo;
import alluxio.master.NoopMaster;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.server.RaftServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.lang.reflect.Method;
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
  public ExpectedException mThrown = ExpectedException.none();

  private RaftJournalSystem mLeaderJournalSystem;
  private RaftJournalSystem mFollowerJournalSystem;

  // A 30sec wait-options object for use by the test.
  private WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(30000);

  @Before
  public void before() throws Exception {
    // Create and start journal systems.
    List<RaftJournalSystem> journalSystems = startJournalCluster(createJournalSystems(2));
    // Sleep for 2 leader election cycles for leadership to stabilize.
    Thread.sleep(2
            * ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT));

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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
            ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT));
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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
    mFollowerJournalSystem.checkpoint();

    // Restart the follower.
    mFollowerJournalSystem.stop();
    mFollowerJournalSystem.start();
    Thread.sleep(ServerConfiguration.getMs(
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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
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
    changeToFollower(mLeaderJournalSystem);
    changeToCandidate(mFollowerJournalSystem);
    CommonUtils.waitFor("follower becomes leader", () -> mFollowerJournalSystem.isLeader(),
        mWaitOptions);
    Assert.assertFalse(mLeaderJournalSystem.isLeader());
    Assert.assertTrue(mFollowerJournalSystem.isLeader());
    mFollowerJournalSystem.gainPrimacy();
  }

  @Test
  public void gainPrimacyDuringCatchup() throws Exception {
    // TODO(feng): remove this test when remote journal write is deprecated
    after();
    try (Closeable r = new ConfigurationRule(
        PropertyKey.MASTER_EMBEDDED_JOURNAL_WRITE_REMOTE_ENABLED, "true",
        ServerConfiguration.global()).toResource()) {
      before();
      // Create a counting master implementation that counts how many journal entries it processed.
      CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
      mFollowerJournalSystem.createJournal(countingMaster);

      // Using a large entry count for catching transition while in-progress.
      final int entryCount = 100000;

      // Suspend follower journal system.
      mFollowerJournalSystem.suspend(null);
      // Catch up follower journal to a large index to be able to transition while in progress.
      final long catchupIndex = entryCount - 5;
      Map<String, Long> backupSequences = new HashMap<>();
      backupSequences.put("FileSystemMaster", catchupIndex);
      CatchupFuture catchupFuture = mFollowerJournalSystem.catchup(backupSequences);

      // Create entries in parallel on the leader journal context.
      // These will be replicated to follower journal context.
      ForkJoinPool.commonPool().submit(() -> {
        try (JournalContext journalContext =
                 mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
          for (int i = 0; i < entryCount; i++) {
            journalContext
                .append(
                    alluxio.proto.journal.Journal.JournalEntry.newBuilder()
                        .setInodeLastModificationTime(
                            File.InodeLastModificationTimeEntry.newBuilder().setId(i).build())
                        .build());
          }
        } catch (Exception e) {
          Assert.fail(String.format("Failed while writing entries: %s", e.toString()));
        }
      });

      // Wait until advancing starts.
      CommonUtils.waitFor("Advancing to start.", () -> countingMaster.getApplyCount() > 0,
          mWaitOptions);

      // Gain primacy in follower journal and validate it catches up.
      mLeaderJournalSystem.notifyLeadershipStateChanged(false);
      mFollowerJournalSystem.notifyLeadershipStateChanged(true);
      mFollowerJournalSystem.gainPrimacy();
      // Can't use countingMaster because Raft stops applying entries for primary journals.
      // Using JournalSystem#getCurrentSequences() API instead.
      CommonUtils.waitFor(
          "full state acquired after resume",
          () -> mFollowerJournalSystem.getCurrentSequenceNumbers()
              .values().stream().distinct().collect(Collectors.toList()).get(0) == entryCount - 1,
          mWaitOptions);

      // Follower should no longer be suspended after becoming primary.
      Assert.assertFalse(mFollowerJournalSystem.isSuspended());
    }
  }

  /**
   * Creates list of raft journal systems in a clustered mode.
   */
  private List<RaftJournalSystem> createJournalSystems(int journalSystemCount) throws Exception {
    // Override defaults for faster quorum formation.
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, 550);
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, 1100);

    List<InetSocketAddress> clusterAddresses = new ArrayList<>(journalSystemCount);
    List<Integer> freePorts = getFreePorts(journalSystemCount);
    for (int i = 0; i < journalSystemCount; i++) {
      clusterAddresses.add(InetSocketAddress.createUnresolved("localhost", freePorts.get(i)));
    }

    List<RaftJournalSystem> journalSystems = new ArrayList<>(journalSystemCount);
    for (int i = 0; i < journalSystemCount; i++) {
      journalSystems.add(RaftJournalSystem.create(RaftJournalConfiguration
          .defaults(NetworkAddressUtils.ServiceType.MASTER_RAFT).setPath(mFolder.newFolder())
          .setClusterAddresses(clusterAddresses).setLocalAddress(clusterAddresses.get(i))));
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
    return RaftJournalSystem.create(RaftJournalConfiguration
        .defaults(NetworkAddressUtils.ServiceType.MASTER_RAFT).setPath(mFolder.newFolder())
        .setClusterAddresses(clusterAddresses).setLocalAddress(joinAddr));
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

  @VisibleForTesting
  void changeToCandidate(RaftJournalSystem journalSystem) throws Exception {
    RaftServer.Division serverImpl = journalSystem.getRaftServer()
            .getDivision(RaftJournalSystem.RAFT_GROUP_ID);
    Class<?> raftServerImpl = (Class.forName("org.apache.ratis.server.impl.RaftServerImpl"));
    Method method = raftServerImpl.getDeclaredMethod("changeToCandidate", boolean.class);
    method.setAccessible(true);
    method.invoke(serverImpl, true);
  }

  @VisibleForTesting
  void changeToFollower(RaftJournalSystem journalSystem) throws Exception {
    RaftServer.Division serverImplObj = journalSystem.getRaftServer()
            .getDivision(RaftJournalSystem.RAFT_GROUP_ID);
    Class<?> raftServerImplClass = Class.forName("org.apache.ratis.server.impl.RaftServerImpl");

    Method getStateMethod = raftServerImplClass.getDeclaredMethod("getState");
    getStateMethod.setAccessible(true);
    Object serverStateObj = getStateMethod.invoke(serverImplObj);
    Class<?> serverStateClass = Class.forName("org.apache.ratis.server.impl.ServerState");
    Method getCurrentTermMethod = serverStateClass.getDeclaredMethod("getCurrentTerm");
    getCurrentTermMethod.setAccessible(true);
    long currentTermObj = (long) getCurrentTermMethod.invoke(serverStateObj);

    Method changeToFollowerMethod = raftServerImplClass.getDeclaredMethod("changeToFollower",
        long.class, boolean.class, Object.class);

    changeToFollowerMethod.setAccessible(true);
    changeToFollowerMethod.invoke(serverImplObj, currentTermObj, true, "test");
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

  /**
   * Used to validate journal apply counts to master.
   */
  class CountingDummyFileSystemMaster extends NoopMaster {
    /** Tracks how many entries have been applied to master. */
    private long mApplyCount = 0;

    @Override
    public boolean processJournalEntry(Journal.JournalEntry entry) {
      mApplyCount++;
      return true;
    }

    @Override
    public void resetState() {
      mApplyCount = 0;
    }

    /**
     * @return how many entries are applied
     */
    public long getApplyCount() {
      return mApplyCount;
    }

    @Override
    public String getName() {
      /**
       * RaftJournalWriter doesn't accept empty journal entries. FileSystemMaster is returned here
       * according to injected entry type during the test.
       */
      return "FileSystemMaster";
    }
  }
}
