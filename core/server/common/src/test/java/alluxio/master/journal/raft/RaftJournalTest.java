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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.NoopMaster;
import alluxio.master.journal.AdvanceFuture;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalSystem;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

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

  private JournalSystem mLeaderJournalSystem;
  private JournalSystem mFollowerJournalSystem;

  @Before
  public void before() throws Exception {
    // Create and start journal systems.
    List<RaftJournalSystem> journalSystems = startJournalCluster(createJournalSystems(2));
    // Sleep for 2 leader election cycles for leadership to stabilize.
    Thread
        .sleep(2 * ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT));

    // Assign references for leader/follower journal systems.
    mLeaderJournalSystem = journalSystems.get(0);
    mFollowerJournalSystem = journalSystems.get(1);
    if (journalSystems.get(1).isLeader()) {
      mLeaderJournalSystem = journalSystems.get(1);
      mFollowerJournalSystem = journalSystems.get(0);
    }
    // Transition primary journal to primacy state.
    // This is required because primary selector is not used in this test.
    mLeaderJournalSystem.gainPrimacy();
  }

  @After
  public void After() throws Exception {
    mLeaderJournalSystem.stop();
    mFollowerJournalSystem.stop();
  }

  @Test
  public void suspendAdvanceResume() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend();
    // Advance follower journal system to target-index:5.
    final long advanceIndex = 5;
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", advanceIndex);
    AdvanceFuture advanceFuture = mFollowerJournalSystem.advance(backupSequences);

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

    // Wait for advanced sequence to applied.
    advanceFuture.waitTermination();
    Assert.assertEquals(advanceIndex + 1, countingMaster.getApplyCount());
    // Wait for 2 heart-beat period and verify follower master state hasn't changed.
    Thread.sleep(
        2 * ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL));
    Assert.assertEquals(advanceIndex + 1, countingMaster.getApplyCount());
    // Exit backup mode and wait until follower master acquires the current knowledge.
    mFollowerJournalSystem.resume();
    CommonUtils.waitFor("full state acquired", () -> countingMaster.getApplyCount() == entryCount);

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
        () -> countingMaster.getApplyCount() == entryCount + 1);
  }

  // Raft journal receives leader knowledge in chunks.
  // So advancing should take into account seeing partial knowledge.
  @Test
  public void advanceInSteps() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend();

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

    // Advance follower journal system to target-index:(fileCount * 2) - 1.
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", (long) (entryBatchCount * 2) - 1);
    AdvanceFuture advanceFuture = mFollowerJournalSystem.advance(backupSequences);

    // Create next batch of entries on the leader journal context.
    try (JournalContext journalContext =
        mLeaderJournalSystem.createJournal(new NoopMaster()).createJournalContext()) {
      for (int i = 0; i < entryBatchCount; i++) {
        journalContext.append(
            alluxio.proto.journal.Journal.JournalEntry.newBuilder().setInodeLastModificationTime(
                File.InodeLastModificationTimeEntry.newBuilder().setId(i).build()).build());
      }
    }

    // Wait for advanced sequence to applied.
    advanceFuture.waitTermination();
    Assert.assertEquals(entryBatchCount * 2, countingMaster.getApplyCount());
  }

  @Test
  public void subsequentAdvances() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend();

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
    // Advance follower journal system to first batch of entries.
    backupSequences.put("FileSystemMaster", (long) entryBatchCount - 1);
    mFollowerJournalSystem.advance(backupSequences).waitTermination();
    // Advance follower journal system to second batch of entries.
    backupSequences.put("FileSystemMaster", (long) (2 * entryBatchCount) - 1);
    mFollowerJournalSystem.advance(backupSequences).waitTermination();

    // Verify master has caught up after advancing.
    Assert.assertEquals(entryBatchCount * 2, countingMaster.getApplyCount());
  }

  @Test
  public void gainPrimacyAfterSuspend() throws Exception {

    // Create a counting master implementation that counts how many journal entries it processed.
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend();

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
    mFollowerJournalSystem.gainPrimacy();
    CommonUtils.waitFor("full state acquired after resume",
        () -> countingMaster.getApplyCount() == entryCount);

    // Resuming should fail after becoming primary.
    mThrown.expect(IllegalStateException.class);
    mFollowerJournalSystem.resume();
  }

  @Test
  public void gainPrimacyAfterAdvance() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend();
    // Advance follower journal system to target-index:5.
    final long advanceIndex = 5;
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", advanceIndex);
    AdvanceFuture advanceFuture = mFollowerJournalSystem.advance(backupSequences);

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

    // Wait until advanced.
    advanceFuture.waitTermination();

    Assert.assertEquals(advanceIndex + 1, countingMaster.getApplyCount());
    // Gain primacy in follower journal and validate it catches up.
    mFollowerJournalSystem.gainPrimacy();
    CommonUtils.waitFor("full state acquired after resume",
        () -> countingMaster.getApplyCount() == entryCount);

    // Resuming should fail after becoming primary.
    mThrown.expect(IllegalStateException.class);
    mFollowerJournalSystem.resume();
  }

  @Test
  public void gainPrimacyDuringAdvance() throws Exception {
    // Create a counting master implementation that counts how many journal entries it processed.
    CountingDummyFileSystemMaster countingMaster = new CountingDummyFileSystemMaster();
    mFollowerJournalSystem.createJournal(countingMaster);

    // Using a large entry count for catching transition while in-progress.
    final int entryCount = 100000;

    // Suspend follower journal system.
    mFollowerJournalSystem.suspend();
    // Advance follower journal to a large index to be able to transition while in progress.
    final long advanceIndex = entryCount - 5;
    Map<String, Long> backupSequences = new HashMap<>();
    backupSequences.put("FileSystemMaster", advanceIndex);
    AdvanceFuture advanceFuture = mFollowerJournalSystem.advance(backupSequences);

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
    CommonUtils.waitFor("Advancing to start.", () -> countingMaster.getApplyCount() > 0);

    // Gain primacy in follower journal and validate it catches up.
    mFollowerJournalSystem.gainPrimacy();
    // Can't use countingMaster because Raft stops applying entries for primary journals.
    // Using JournalSystem#getCurrentSequences() API instead.
    CommonUtils.waitFor("full state acquired after resume",
        () -> mFollowerJournalSystem.getCurrentSequences().values().stream().distinct()
            .collect(Collectors.toList()).get(0) == entryCount - 1);

    // Resuming should fail after becoming primary.
    mThrown.expect(IllegalStateException.class);
    mFollowerJournalSystem.resume();
  }

  /**
   * Creates list of raft journal systems in a clustered mode.
   */
  private List<RaftJournalSystem> createJournalSystems(int journalSystemCount) throws Exception {
    // Override defaults for faster quorum formation.
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT, 550);
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL, 250);

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
