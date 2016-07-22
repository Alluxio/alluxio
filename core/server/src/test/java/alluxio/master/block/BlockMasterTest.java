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

package alluxio.master.block;

import alluxio.Constants;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.AlluxioException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.block.meta.MasterBlockInfo;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.thrift.Command;
import alluxio.thrift.CommandType;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link alluxio.master.block.BlockMaster}.
 */
public class BlockMasterTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(83).setDataPort(84).setWebPort(85);

  private BlockMaster mMaster;
  private PrivateAccess mPrivateAccess;
  private TestClock mClock;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_LOST_WORKER_DETECTION);

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    mClock = new TestClock();
    mMaster = new BlockMaster(blockJournal, mClock);
    mMaster.start(true);
    mPrivateAccess = new PrivateAccess(mMaster);
  }

  /**
   * Stops the master after a test ran.
   */
  @After
  public void after() throws Exception {
    mMaster.stop();
  }

  @Test
  public void countBytes() throws Exception {
    // Register two workers
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mMaster.getWorkerId(NET_ADDRESS_2);
    List<String> tiers = Arrays.asList("MEM", "SSD");
    Map<String, Long> worker1TotalBytesOnTiers = ImmutableMap.of("MEM", 10L, "SSD", 20L);
    Map<String, Long> worker2TotalBytesOnTiers = ImmutableMap.of("MEM", 1000L, "SSD", 2000L);
    Map<String, Long> worker1UsedBytesOnTiers = ImmutableMap.of("MEM", 1L, "SSD", 2L);
    Map<String, Long> worker2UsedBytesOnTiers = ImmutableMap.of("MEM", 100L, "SSD", 200L);
    Map<String, List<Long>> noExistingBlocks = new HashMap<>();
    mMaster.workerRegister(worker1, tiers, worker1TotalBytesOnTiers, worker1UsedBytesOnTiers,
        noExistingBlocks);
    mMaster.workerRegister(worker2, tiers, worker2TotalBytesOnTiers, worker2UsedBytesOnTiers,
        noExistingBlocks);

    // Check that byte counts are summed correctly.
    Assert.assertEquals(3030, mMaster.getCapacityBytes());
    Assert.assertEquals(303L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableMap.of("MEM", 1010L, "SSD", 2020L),
        mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableMap.of("MEM", 101L, "SSD", 202L),
        mMaster.getUsedBytesOnTiers());
  }

  @Test
  public void detectLostWorkers() throws Exception {
    // Register a worker.
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    mMaster.workerRegister(worker1,
        ImmutableList.of("MEM"),
        ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 10L),
        ImmutableMap.<String, List<Long>> of());

    // Advance the block master's clock by an hour so that worker appears lost.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1, TimeUnit.SECONDS);
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1, TimeUnit.SECONDS);

    // Make sure the worker is detected as lost.
    Set<WorkerInfo> info = mMaster.getLostWorkersInfo();
    Assert.assertEquals(worker1, Iterables.getOnlyElement(info).getId());
  }

  @Test
  public void workerReregisterRemembersLostWorker() throws Exception {
    // Register a worker.
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    mMaster.workerRegister(worker1,
        ImmutableList.of("MEM"),
        ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 10L),
        ImmutableMap.<String, List<Long>> of());

    // Advance the block master's clock by an hour so that the worker appears lost.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1, TimeUnit.SECONDS);
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1, TimeUnit.SECONDS);

    // Reregister the worker.
    mMaster.getWorkerId(NET_ADDRESS_1);
    mMaster.workerRegister(worker1,
        ImmutableList.of("MEM"),
        ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 10L),
        ImmutableMap.<String, List<Long>> of());

    // Check that there are no longer any lost workers and there is a live worker.
    Assert.assertEquals(1, mMaster.getWorkerCount());
    Assert.assertEquals(0, mMaster.getLostWorkersInfo().size());
  }

  @Test
  public void removeBlock() throws Exception {
    // Create a worker with a block.
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    mMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), ImmutableMap.<String, List<Long>>of());
    mMaster.commitBlock(worker1, 50L, "MEM", blockId, 20L);

    // Remove the block
    mMaster.removeBlocks(Arrays.asList(1L), /*delete=*/false );

    // Check that the worker heartbeat tells the worker to remove the block.
    Map<String, Long> memUsage = ImmutableMap.of("MEM", 0L);
    List<Long> removedBlocks = ImmutableList.<Long>of();
    Map<String, List<Long>> addedBlocks = ImmutableMap.of();
    Command heartBeat = mMaster.workerHeartbeat(worker1, memUsage, removedBlocks, addedBlocks);
    Assert.assertEquals(ImmutableList.of(1L), heartBeat.getData());
  }

  /**
   * Tests the {@link BlockMaster#workerHeartbeat(long, Map, List, Map)} method where the master
   * tells the worker to remove a block.
   */
  @Test
  public void workerHeartbeatTest() throws Exception {
    long workerId = mMaster.getWorkerId(NET_ADDRESS_1);

    MasterWorkerInfo workerInfo = mPrivateAccess.getWorkerById(workerId);
    final Map<String, Long> USED_BYTES_ON_TIERS = ImmutableMap.of("MEM", 125L);
    final List<Long> INITIAL_BLOCKS = ImmutableList.of(1L, 2L);
    addWorker(mMaster, workerId, Arrays.asList("MEM"), ImmutableMap.of("MEM", 500L),
        USED_BYTES_ON_TIERS);
    for (Long block : INITIAL_BLOCKS) {
      mMaster.commitBlock(workerId, USED_BYTES_ON_TIERS.get("MEM"), "MEM", block, 100L);
    }

    // test heartbeat removing a block
    Assert.assertEquals(ImmutableSet.copyOf(INITIAL_BLOCKS), workerInfo.getBlocks());
    final long REMOVED_BLOCK = INITIAL_BLOCKS.get(0);
    Command heartBeat1 = mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS,
        ImmutableList.of(REMOVED_BLOCK), ImmutableMap.<String, List<Long>>of());
    Set<Long> expectedBlocks =
        Sets.difference(ImmutableSet.copyOf(INITIAL_BLOCKS), ImmutableSet.of(REMOVED_BLOCK));
    // block is removed from worker info
    Assert.assertEquals(expectedBlocks, workerInfo.getBlocks());
    // worker is removed from block info
    Assert.assertEquals(ImmutableSet.of(),
        mPrivateAccess.getMasterBlockInfo(REMOVED_BLOCK).getWorkers());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat1);

    // test heartbeat adding back the block
    List<Long> readdedBlocks = ImmutableList.of(REMOVED_BLOCK);
    Command heartBeat2 = mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS,
        ImmutableList.<Long>of(), ImmutableMap.of("MEM", readdedBlocks));
    // block is restored to worker info
    Assert.assertEquals(ImmutableSet.copyOf(INITIAL_BLOCKS), workerInfo.getBlocks());
    // worker is restored to block info
    Assert.assertEquals(ImmutableSet.of(workerId),
        mPrivateAccess.getMasterBlockInfo(REMOVED_BLOCK).getWorkers());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat2);

    // test heartbeat where the master tells the worker to remove a block
    final long BLOCK_TO_FREE = INITIAL_BLOCKS.get(1);
    workerInfo.updateToRemovedBlock(true, BLOCK_TO_FREE);
    Command heartBeat3 = mMaster.workerHeartbeat(workerId, USED_BYTES_ON_TIERS,
        ImmutableList.<Long>of(), ImmutableMap.<String, List<Long>>of());
    Assert.assertEquals(new Command(CommandType.Free, ImmutableList.of(BLOCK_TO_FREE)),
        heartBeat3);
  }

  /**
   * Tests the {@link BlockMaster#workerHeartbeat(long, Map, List, Map)} method.
   */
  @Test
  public void heartbeatStatusTest() throws Exception {
    long workerId = mMaster.getWorkerId(NET_ADDRESS_1);

    MasterWorkerInfo workerInfo = mPrivateAccess.getWorkerById(workerId);
    final Map<String, Long> INITIAL_USED_BYTES_ON_TIERS =
        ImmutableMap.of("MEM", 25L, "SSD", 50L, "HDD", 125L);
    addWorker(mMaster, workerId, Arrays.asList("MEM", "SSD", "HDD"),
        ImmutableMap.of("MEM", 50L, "SSD", 100L, "HDD", 500L), INITIAL_USED_BYTES_ON_TIERS);

    long lastUpdatedTime1 = workerInfo.getLastUpdatedTimeMs();
    Thread.sleep(1); // sleep for 1ms so that lastUpdatedTimeMs is guaranteed to change
    final Map<String, Long> NEW_USED_BYTES_ON_TIERS =
        ImmutableMap.of("MEM", 50L, "SSD", 100L, "HDD", 500L);
    // test simple heartbeat letting the master know that more bytes are being used
    Command heartBeat = mMaster.workerHeartbeat(workerId, NEW_USED_BYTES_ON_TIERS,
        ImmutableList.<Long>of(), ImmutableMap.<String, List<Long>>of());
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat);
    // updates the number of used bytes on the worker
    Assert.assertEquals(NEW_USED_BYTES_ON_TIERS, workerInfo.getUsedBytesOnTiers());
    // updates the workers last updated time
    Assert.assertNotEquals(lastUpdatedTime1, workerInfo.getLastUpdatedTimeMs());
  }

  /**
   * Tests the {@link BlockMaster#workerHeartbeat(long, Map, List, Map)} with an unknown worker.
   */
  @Test
  public void unknownHeartbeatTest() {
    Command heartBeat = mMaster.workerHeartbeat(0, null, null, null);
    Assert.assertEquals(new Command(CommandType.Register, ImmutableList.<Long>of()), heartBeat);
  }

  /**
   * Tests the {@link BlockMaster#stop()} method.
   */
  @Test
  public void stopTest() throws Exception {
    ExecutorService service = Whitebox.getInternalState(mMaster, "mExecutorService");
    Future<?> lostWorkerThread = Whitebox.getInternalState(mMaster, "mLostWorkerDetectionService");
    Assert.assertFalse(lostWorkerThread.isDone());
    Assert.assertFalse(service.isShutdown());
    mMaster.stop();
    Assert.assertTrue(lostWorkerThread.isDone());
    Assert.assertTrue(service.isShutdown());
  }

  private void addWorker(BlockMaster master, long workerId, List<String> storageTierAliases,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers)
          throws AlluxioException {
    master.workerRegister(workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
        new HashMap<String, List<Long>>());
  }

  /** Private access to {@link BlockMaster} internals. */
  private class PrivateAccess {
    private final Map<Long, MasterBlockInfo> mBlocks;
    private final IndexDefinition<MasterWorkerInfo> mIdIndex;
    private final IndexedSet<MasterWorkerInfo> mWorkers;

    PrivateAccess(BlockMaster blockMaster) {
      mBlocks = Whitebox.getInternalState(mMaster, "mBlocks");
      mIdIndex = Whitebox.getInternalState(BlockMaster.class, "ID_INDEX");
      mWorkers = Whitebox.getInternalState(mMaster, "mWorkers");
    }

    /**
     * Looks up the {@link MasterWorkerInfo} for a given worker id.
     *
     * @param workerId the worker id to look up
     * @return the {@link MasterWorkerInfo} for the given workerId
     */
    private MasterWorkerInfo getWorkerById(long workerId) {
      synchronized (mWorkers) {
        return mWorkers.getFirstByField(mIdIndex, workerId);
      }
    }

    /**
     * Looks up the {@link MasterBlockInfo} for the given block id.
     *
     * @param blockId the block id
     * @return the {@link MasterBlockInfo}
     */
    public MasterBlockInfo getMasterBlockInfo(long blockId) {
      synchronized (mBlocks) {
        return mBlocks.get(blockId);
      }
    }
  }
}
