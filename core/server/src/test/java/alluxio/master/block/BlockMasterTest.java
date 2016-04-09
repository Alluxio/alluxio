/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.block;

import alluxio.collections.IndexedSet;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
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
import com.google.common.collect.Maps;
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
   *
   * @throws Exception if the test folder cannot be created or the master fails to start
   */
  @Before
  public void before() throws Exception {
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    mMaster = new BlockMaster(blockJournal);
    mMaster.start(true);
    mPrivateAccess = new PrivateAccess(mMaster);
  }

  /**
   * Stops the master after a test ran.
   *
   * @throws Exception if the master fails to stop
   */
  @After
  public void after() throws Exception {
    mMaster.stop();
  }

  /**
   * Tests the different different byte methods of the {@link BlockMaster}.
   *
   * @throws Exception if adding a worker fails
   */
  @Test
  public void countBytesTest() throws Exception {
    Assert.assertEquals(0L, mMaster.getCapacityBytes());
    Assert.assertEquals(0L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableMap.of(), mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableMap.of(), mMaster.getUsedBytesOnTiers());
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mMaster.getWorkerId(NET_ADDRESS_2);
    addWorker(mMaster, worker1, Arrays.asList("MEM", "SSD", "HDD"),
        ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD", 30L),
        ImmutableMap.of("MEM", 20L, "SSD", 50L, "HDD", 10L));
    Assert.assertEquals(330L, mMaster.getCapacityBytes());
    Assert.assertEquals(80L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableMap.of("MEM", 100L, "SSD", 200L, "HDD", 30L),
        mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableMap.of("MEM", 20L, "SSD", 50L, "HDD", 10L),
        mMaster.getUsedBytesOnTiers());
    addWorker(mMaster, worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 500L),
        ImmutableMap.of("MEM", 300L));
    Assert.assertEquals(830L, mMaster.getCapacityBytes());
    Assert.assertEquals(380L, mMaster.getUsedBytes());
    Assert.assertEquals(ImmutableMap.of("MEM", 600L, "SSD", 200L, "HDD", 30L),
        mMaster.getTotalBytesOnTiers());
    Assert.assertEquals(ImmutableMap.of("MEM", 320L, "SSD", 50L, "HDD", 10L),
        mMaster.getUsedBytesOnTiers());
  }

  /**
   * Tests the {@link BlockMaster#getLostWorkersInfo()} method.
   */
  @Test
  public void getLostWorkersInfoTest() {
    MasterWorkerInfo workerInfo1 = new MasterWorkerInfo(1, NET_ADDRESS_1);
    MasterWorkerInfo workerInfo2 = new MasterWorkerInfo(2, NET_ADDRESS_2);
    mPrivateAccess.addLostWorker(workerInfo1);
    Assert.assertEquals(ImmutableSet.of(workerInfo1.generateClientWorkerInfo()),
        mMaster.getLostWorkersInfo());
    mPrivateAccess.addLostWorker(workerInfo2);

    final Set<WorkerInfo> expected = ImmutableSet.of(workerInfo1.generateClientWorkerInfo(),
        workerInfo2.generateClientWorkerInfo());

    Assert.assertEquals(expected, mMaster.getLostWorkersInfo());
  }

  /**
   * Tests that after {@link PrivateAccess#addLostWorker(MasterWorkerInfo)} a worker can be
   * registered via {@link BlockMaster#workerRegister(long, List, Map, Map, Map)}.
   *
   * @throws Exception if registering a worker fails
   */
  @Test
  public void registerLostWorkerTest() throws Exception {
    final WorkerNetAddress na = NET_ADDRESS_1;
    final long expectedId = 1;
    final MasterWorkerInfo workerInfo1 = new MasterWorkerInfo(expectedId, na);

    workerInfo1.addBlock(1L);
    mPrivateAccess.addLostWorker(workerInfo1);
    final long workerId = mMaster.getWorkerId(na);
    Assert.assertEquals(expectedId, workerId);

    final List<Long> blocks = ImmutableList.of(42L);
    mMaster.workerRegister(workerId, Arrays.asList("MEM"), ImmutableMap.of("MEM", 1024L),
        ImmutableMap.of("MEM", 1024L), ImmutableMap.of("MEM", blocks));

    final Set<Long> expectedBlocks = ImmutableSet.of(42L);
    final Set<Long> actualBlocks = workerInfo1.getBlocks();

    Assert.assertEquals("The master should reflect the blocks declared at registration",
        expectedBlocks, actualBlocks);
  }

  /**
   * Tests the {@link BlockMaster#removeBlocks(List, boolean)} method.
   *
   * @throws Exception if registering a worker fails
   */
  @Test
  public void removeBlocksTest() throws Exception {
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mMaster.getWorkerId(NET_ADDRESS_1);
    MasterWorkerInfo workerInfo1 = mPrivateAccess.getWorkerById(worker1);
    MasterWorkerInfo workerInfo2 = mPrivateAccess.getWorkerById(worker2);
    List<Long> workerBlocks = Arrays.asList(1L, 2L, 3L);
    HashMap<String, List<Long>> noBlocksInTiers = Maps.newHashMap();
    mMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), noBlocksInTiers);
    mMaster.workerRegister(worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), noBlocksInTiers);
    mMaster.commitBlock(worker1, 1L, "MEM", 1L, 1L);
    mMaster.commitBlock(worker1, 2L, "MEM", 2L, 1L);
    mMaster.commitBlock(worker1, 3L, "MEM", 3L, 1L);
    mMaster.commitBlock(worker2, 1L, "MEM", 1L, 1L);
    mMaster.commitBlock(worker2, 2L, "MEM", 2L, 1L);
    mMaster.commitBlock(worker2, 3L, "MEM", 3L, 1L);
    mMaster.removeBlocks(workerBlocks, false /* delete */);
    Assert.assertEquals(1L, mMaster.getBlockInfo(1L).getBlockId());

    // Test removeBlocks with delete
    mMaster.removeBlocks(workerBlocks, true /* delete */);

    // Update the heartbeat of removedBlockIds received from worker 1
    Command heartBeat1 = mMaster.workerHeartbeat(worker1,
        ImmutableMap.of("MEM", 20L, "SSD", 30L, "HDD", 50L),
        ImmutableList.of(1L, 2L, 3L), ImmutableMap.<String, List<Long>>of());
    // Verify removedBlockIds have been removed from ToRemoveBlocks on worker 1
    Assert.assertFalse(workerInfo1.getToRemoveBlocks().contains(1L));
    Assert.assertFalse(workerInfo1.getToRemoveBlocks().contains(2L));
    Assert.assertFalse(workerInfo1.getToRemoveBlocks().contains(3L));
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat1);

    // Update the heartbeat of removedBlockIds received from worker 2
    Command heartBeat2 = mMaster.workerHeartbeat(worker2,
        ImmutableMap.of("MEM", 30L, "SSD", 50L, "HDD", 60L),
        ImmutableList.of(1L, 2L, 3L), ImmutableMap.<String, List<Long>>of());
    // Verify removedBlockIds have been removed from ToRemoveBlocks on worker2
    Assert.assertFalse(workerInfo2.getToRemoveBlocks().contains(1L));
    Assert.assertFalse(workerInfo2.getToRemoveBlocks().contains(2L));
    Assert.assertFalse(workerInfo2.getToRemoveBlocks().contains(3L));
    // Verify the muted Free command on worker2
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat2);

    mThrown.expect(BlockInfoException.class);
    mMaster.getBlockInfo(1L);
    Assert.assertFalse(mMaster.getLostBlocks().contains(1L));
    Assert.assertFalse(mMaster.getLostBlocks().contains(2L));
    Assert.assertFalse(mMaster.getLostBlocks().contains(3L));
  }

  /**
   * Tests the {@link BlockMaster#workerHeartbeat(long, Map, List, Map)} method where the master
   * tells the worker to remove a block.
   *
   * @throws Exception if adding a worker fails
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
    Assert.assertEquals(new Command(CommandType.Free, ImmutableList.<Long>of(BLOCK_TO_FREE)),
        heartBeat3);
  }

  /**
   * Tests the {@link BlockMaster#workerHeartbeat(long, Map, List, Map)} method.
   *
   * @throws Exception if adding a worker fails
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
   * Tests the {@link HeartbeatContext#MASTER_LOST_WORKER_DETECTION} to detect a lost worker.
   *
   * @throws Exception if waiting for the detector fails
   */
  @Test
  public void detectLostWorkerTest() throws Exception {
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 5, TimeUnit.SECONDS);

    // Get a new worker id.
    long workerId = mMaster.getWorkerId(NET_ADDRESS_1);
    MasterWorkerInfo workerInfo = mPrivateAccess.getWorkerById(workerId);
    Assert.assertNotNull(workerInfo);

    // Run the lost worker detector.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1,
        TimeUnit.SECONDS));

    // No workers should be considered lost.
    Assert.assertEquals(0, mMaster.getLostWorkersInfo().size());
    Assert.assertNotNull(mPrivateAccess.getWorkerById(workerId));

    // Set the last updated time for the worker to be definitely too old, so it is considered lost.
    // TODO(andrew): Create a src/test PublicAccess to MasterWorkerInfo internals and replace this
    Whitebox.setInternalState(workerInfo, "mLastUpdatedTimeMs", 0);

    // Run the lost worker detector.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1,
        TimeUnit.SECONDS));

    // There should be a lost worker.
    Assert.assertEquals(1, mMaster.getLostWorkersInfo().size());
    Assert.assertNull(mPrivateAccess.getWorkerById(workerId));

    // Get the worker id again, simulating the lost worker re-registering.
    workerId = mMaster.getWorkerId(NET_ADDRESS_1);
    Assert.assertNotNull(mPrivateAccess.getWorkerById(workerId));

    // Run the lost worker detector.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1,
        TimeUnit.SECONDS));

    // No workers should be considered lost.
    Assert.assertEquals(0, mMaster.getLostWorkersInfo().size());
    Assert.assertNotNull(mPrivateAccess.getWorkerById(workerId));
  }

  /**
   * Tests the {@link BlockMaster#stop()} method.
   *
   * @throws Exception if stopping the master fails
   */
  @Test
  public void stopTest() throws Exception {
    ExecutorService service =
        (ExecutorService) Whitebox.getInternalState(mMaster, "mExecutorService");
    Future<?> lostWorkerThread =
        (Future<?>) Whitebox.getInternalState(mMaster, "mLostWorkerDetectionService");
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
        Maps.<String, List<Long>>newHashMap());
  }

  /** Private access to {@link BlockMaster} internals. */
  private class PrivateAccess {
    private final Map<Long, MasterBlockInfo> mBlocks;
    private final IndexedSet.FieldIndex<MasterWorkerInfo> mIdIndex;
    private final IndexedSet<MasterWorkerInfo> mLostWorkers;
    private final IndexedSet<MasterWorkerInfo> mWorkers;

    PrivateAccess(BlockMaster blockMaster) {
      mBlocks = Whitebox.getInternalState(mMaster, "mBlocks");
      mIdIndex = Whitebox.getInternalState(mMaster, "mIdIndex");
      mLostWorkers = Whitebox.getInternalState(mMaster, "mLostWorkers");
      mWorkers = Whitebox.getInternalState(mMaster, "mWorkers");
    }

    /**
     * @param worker a {@link MasterWorkerInfo} to add to the list of lost workers
     */
    private void addLostWorker(MasterWorkerInfo worker) {
      synchronized (mWorkers) {
        mLostWorkers.add(worker);
      }
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
