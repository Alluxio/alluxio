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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.clock.ManualClock;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.TestSafeModeManager;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.Metric;
import alluxio.thrift.Command;
import alluxio.thrift.CommandType;
import alluxio.thrift.RegisterWorkerTOptions;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit tests for {@link BlockMaster}.
 */
public class BlockMasterTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(83).setDataPort(84).setWebPort(85);

  private static final List<Long> NO_BLOCKS = ImmutableList.of();
  private static final Map<String, List<Long>> NO_BLOCKS_ON_TIERS = ImmutableMap.of();

  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private SafeModeManager mSafeModeManager;
<<<<<<< HEAD
  private long mStartTimeMs;
=======
  private MetricsMaster mMetricsMaster;
  private List<Metric> mMetrics;
>>>>>>> master

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
    mRegistry = new MasterRegistry();
    mSafeModeManager = new TestSafeModeManager();
<<<<<<< HEAD
    mStartTimeMs = System.currentTimeMillis();
=======
    mMetrics = Lists.newArrayList();
>>>>>>> master
    JournalSystem journalSystem = new NoopJournalSystem();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, journalSystem, mSafeModeManager);
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
<<<<<<< HEAD
    mBlockMaster = new DefaultBlockMaster(
        new MasterContext(journalSystem, mSafeModeManager, mStartTimeMs),
        mClock, ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
=======
    mBlockMaster =
        new DefaultBlockMaster(mMetricsMaster, new MasterContext(journalSystem, mSafeModeManager),
            mClock, ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
>>>>>>> master
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);
  }

  /**
   * Stops the master after a test ran.
   */
  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  @Test
  public void countBytes() throws Exception {
    // Register two workers
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    List<String> tiers = Arrays.asList("MEM", "SSD");
    Map<String, Long> worker1TotalBytesOnTiers = ImmutableMap.of("MEM", 10L, "SSD", 20L);
    Map<String, Long> worker2TotalBytesOnTiers = ImmutableMap.of("MEM", 1000L, "SSD", 2000L);
    Map<String, Long> worker1UsedBytesOnTiers = ImmutableMap.of("MEM", 1L, "SSD", 2L);
    Map<String, Long> worker2UsedBytesOnTiers = ImmutableMap.of("MEM", 100L, "SSD", 200L);
    mBlockMaster.workerRegister(worker1, tiers, worker1TotalBytesOnTiers, worker1UsedBytesOnTiers,
        NO_BLOCKS_ON_TIERS, new RegisterWorkerTOptions());
    mBlockMaster.workerRegister(worker2, tiers, worker2TotalBytesOnTiers, worker2UsedBytesOnTiers,
        NO_BLOCKS_ON_TIERS, new RegisterWorkerTOptions());

    // Check that byte counts are summed correctly.
    assertEquals(3030, mBlockMaster.getCapacityBytes());
    assertEquals(303L, mBlockMaster.getUsedBytes());
    assertEquals(ImmutableMap.of("MEM", 1010L, "SSD", 2020L),
        mBlockMaster.getTotalBytesOnTiers());
    assertEquals(ImmutableMap.of("MEM", 101L, "SSD", 202L),
        mBlockMaster.getUsedBytesOnTiers());
  }

  @Test
  public void detectLostWorkers() throws Exception {
    // Register a worker.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1,
        ImmutableList.of("MEM"),
        ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 10L),
        NO_BLOCKS_ON_TIERS,
        new RegisterWorkerTOptions());

    // Advance the block master's clock by an hour so that worker appears lost.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);

    // Make sure the worker is detected as lost.
    List<WorkerInfo> info = mBlockMaster.getLostWorkersInfoList();
    assertEquals(worker1, Iterables.getOnlyElement(info).getId());
  }

  @Test
  public void workerReregisterRemembersLostWorker() throws Exception {
    // Register a worker.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1,
        ImmutableList.of("MEM"),
        ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 10L),
        NO_BLOCKS_ON_TIERS,
        new RegisterWorkerTOptions());

    // Advance the block master's clock by an hour so that the worker appears lost.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);

    // Reregister the worker using its original worker id.
    mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1,
        ImmutableList.of("MEM"),
        ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 10L),
        NO_BLOCKS_ON_TIERS,
        new RegisterWorkerTOptions());

    // Check that there are no longer any lost workers and there is a live worker.
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(0, mBlockMaster.getLostWorkersInfoList().size());
  }

  @Test
  public void removeBlockTellsWorkersToRemoveTheBlock() throws Exception {
    // Create a worker with a block.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    mBlockMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS,  new RegisterWorkerTOptions());
    mBlockMaster.commitBlock(worker1, 50L, "MEM", blockId, 20L);

    // Remove the block
    mBlockMaster.removeBlocks(Arrays.asList(1L), /*delete=*/false);

    // Check that the worker heartbeat tells the worker to remove the block.
    Map<String, Long> memUsage = ImmutableMap.of("MEM", 0L);
    Command heartBeat = mBlockMaster.workerHeartbeat(worker1, memUsage, NO_BLOCKS,
        NO_BLOCKS_ON_TIERS, mMetrics);
    assertEquals(ImmutableList.of(1L), heartBeat.getData());
  }

  @Test
  public void registerCleansUpOrphanedBlocks() throws Exception {
    // Create a worker with unknown blocks.
    long worker = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<Long> orphanedBlocks = Arrays.asList(1L, 2L);
    Map<String, Long> memUsage = ImmutableMap.of("MEM", 10L);
    mBlockMaster.workerRegister(worker, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        memUsage, ImmutableMap.of("MEM", orphanedBlocks), new RegisterWorkerTOptions());

    // Check that the worker heartbeat tells the worker to remove the blocks.
    Command heartBeat = mBlockMaster.workerHeartbeat(worker, memUsage, NO_BLOCKS,
        NO_BLOCKS_ON_TIERS, mMetrics);
    assertEquals(orphanedBlocks, heartBeat.getData());
  }

  @Test
  public void workerHeartbeatUpdatesMemoryCount() throws Exception {
    // Create a worker.
    long worker = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    Map<String, Long> initialUsedBytesOnTiers = ImmutableMap.of("MEM", 50L);
    mBlockMaster.workerRegister(worker, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        initialUsedBytesOnTiers, NO_BLOCKS_ON_TIERS, new RegisterWorkerTOptions());

    // Update used bytes with a worker heartbeat.
    Map<String, Long> newUsedBytesOnTiers = ImmutableMap.of("MEM", 50L);
    mBlockMaster.workerHeartbeat(worker, newUsedBytesOnTiers, NO_BLOCKS, NO_BLOCKS_ON_TIERS,
        mMetrics);

    WorkerInfo workerInfo = Iterables.getOnlyElement(mBlockMaster.getWorkerInfoList());
    assertEquals(50, workerInfo.getUsedBytes());
  }

  @Test
  public void workerHeartbeatUpdatesRemovedBlocks() throws Exception {
    // Create a worker.
    long worker = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS, new RegisterWorkerTOptions());
    long blockId = 1L;
    mBlockMaster.commitBlock(worker, 50L, "MEM", blockId, 20L);

    // Indicate that blockId is removed on the worker.
    mBlockMaster.workerHeartbeat(worker, ImmutableMap.of("MEM", 0L), ImmutableList.of(blockId),
        NO_BLOCKS_ON_TIERS, mMetrics);
    assertTrue(mBlockMaster.getBlockInfo(blockId).getLocations().isEmpty());
  }

  @Test
  public void workerHeartbeatUpdatesAddedBlocks() throws Exception {
    // Create two workers.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS, new RegisterWorkerTOptions());
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    mBlockMaster.workerRegister(worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS, new RegisterWorkerTOptions());

    // Commit blockId to worker1.
    long blockId = 1L;
    mBlockMaster.commitBlock(worker1, 50L, "MEM", blockId, 20L);

    // Send a heartbeat from worker2 saying that it's added blockId.
    List<Long> addedBlocks = ImmutableList.of(blockId);
    mBlockMaster.workerHeartbeat(worker2, ImmutableMap.of("MEM", 0L), NO_BLOCKS,
        ImmutableMap.of("MEM", addedBlocks), mMetrics);

    // The block now has two locations.
    assertEquals(2, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  @Test
  public void unknownWorkerHeartbeatTriggersRegisterRequest() {
    Command heartBeat = mBlockMaster.workerHeartbeat(0, null, null, null, mMetrics);
    assertEquals(new Command(CommandType.Register, ImmutableList.<Long>of()), heartBeat);
  }

  @Test
  public void stopTerminatesExecutorService() throws Exception {
    mBlockMaster.stop();
    assertTrue(mExecutorService.isTerminated());
  }

  @Test
  public void getBlockInfo() throws Exception {
    // Create a worker with a block.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    long blockLength = 20L;
    mBlockMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS, new RegisterWorkerTOptions());
    mBlockMaster.commitBlock(worker1, 50L, "MEM", blockId, blockLength);

    BlockLocation blockLocation = new BlockLocation()
        .setTierAlias("MEM")
        .setWorkerAddress(NET_ADDRESS_1)
        .setWorkerId(worker1);
    BlockInfo expectedBlockInfo = new BlockInfo()
        .setBlockId(1L)
        .setLength(20L)
        .setLocations(ImmutableList.of(blockLocation));
    assertEquals(expectedBlockInfo, mBlockMaster.getBlockInfo(blockId));
  }

  @Test
  public void stop() throws Exception {
    mRegistry.stop();
    assertTrue(mExecutorService.isShutdown());
    assertTrue(mExecutorService.isTerminated());
  }
}
