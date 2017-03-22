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
import alluxio.clock.ManualClock;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterRegistry;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.MutableJournal;
import alluxio.thrift.Command;
import alluxio.thrift.CommandType;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit tests for {@link alluxio.master.block.BlockMaster}.
 */
public class BlockMasterTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(83).setDataPort(84).setWebPort(85);

  private static final List<Long> NO_BLOCKS = ImmutableList.of();
  private static final Map<String, List<Long>> NO_BLOCKS_ON_TIERS = ImmutableMap.of();

  private BlockMaster mMaster;
  private ManualClock mClock;
  private ExecutorService mExecutorService;

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
    MasterRegistry registry = new MasterRegistry();
    JournalFactory factory =
        new MutableJournal.Factory(new URI(mTestFolder.newFolder().getAbsolutePath()));
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mMaster = new BlockMaster(registry, factory, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mMaster.start(true);
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
    mMaster.workerRegister(worker1, tiers, worker1TotalBytesOnTiers, worker1UsedBytesOnTiers,
        NO_BLOCKS_ON_TIERS);
    mMaster.workerRegister(worker2, tiers, worker2TotalBytesOnTiers, worker2UsedBytesOnTiers,
        NO_BLOCKS_ON_TIERS);

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
        NO_BLOCKS_ON_TIERS);

    // Advance the block master's clock by an hour so that worker appears lost.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);

    // Make sure the worker is detected as lost.
    List<WorkerInfo> info = mMaster.getLostWorkersInfoList();
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
        NO_BLOCKS_ON_TIERS);

    // Advance the block master's clock by an hour so that the worker appears lost.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);

    // Reregister the worker using its original worker id.
    mMaster.getWorkerId(NET_ADDRESS_1);
    mMaster.workerRegister(worker1,
        ImmutableList.of("MEM"),
        ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 10L),
        NO_BLOCKS_ON_TIERS);

    // Check that there are no longer any lost workers and there is a live worker.
    Assert.assertEquals(1, mMaster.getWorkerCount());
    Assert.assertEquals(0, mMaster.getLostWorkersInfoList().size());
  }

  @Test
  public void removeBlockTellsWorkersToRemoveTheBlock() throws Exception {
    // Create a worker with a block.
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    mMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS);
    mMaster.commitBlock(worker1, 50L, "MEM", blockId, 20L);

    // Remove the block
    mMaster.removeBlocks(Arrays.asList(1L), /*delete=*/false);

    // Check that the worker heartbeat tells the worker to remove the block.
    Map<String, Long> memUsage = ImmutableMap.of("MEM", 0L);
    Command heartBeat = mMaster.workerHeartbeat(worker1, memUsage, NO_BLOCKS, NO_BLOCKS_ON_TIERS);
    Assert.assertEquals(ImmutableList.of(1L), heartBeat.getData());
  }

  @Test
  public void workerHeartbeatUpdatesMemoryCount() throws Exception {
    // Create a worker.
    long worker = mMaster.getWorkerId(NET_ADDRESS_1);
    Map<String, Long> initialUsedBytesOnTiers = ImmutableMap.of("MEM", 50L);
    mMaster.workerRegister(worker, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        initialUsedBytesOnTiers, NO_BLOCKS_ON_TIERS);

    // Update used bytes with a worker heartbeat.
    Map<String, Long> newUsedBytesOnTiers = ImmutableMap.of("MEM", 50L);
    mMaster.workerHeartbeat(worker, newUsedBytesOnTiers, NO_BLOCKS, NO_BLOCKS_ON_TIERS);

    WorkerInfo workerInfo = Iterables.getOnlyElement(mMaster.getWorkerInfoList());
    Assert.assertEquals(50, workerInfo.getUsedBytes());
  }

  @Test
  public void workerHeartbeatUpdatesRemovedBlocks() throws Exception {
    // Create a worker.
    long worker = mMaster.getWorkerId(NET_ADDRESS_1);
    mMaster.workerRegister(worker, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS);
    long blockId = 1L;
    mMaster.commitBlock(worker, 50L, "MEM", blockId, 20L);

    // Indicate that blockId is removed on the worker.
    mMaster.workerHeartbeat(worker, ImmutableMap.of("MEM", 0L), ImmutableList.of(blockId),
        NO_BLOCKS_ON_TIERS);
    Assert.assertTrue(mMaster.getBlockInfo(blockId).getLocations().isEmpty());
  }

  @Test
  public void workerHeartbeatUpdatesAddedBlocks() throws Exception {
    // Create two workers.
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    mMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS);
    long worker2 = mMaster.getWorkerId(NET_ADDRESS_2);
    mMaster.workerRegister(worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS);

    // Commit blockId to worker1.
    long blockId = 1L;
    mMaster.commitBlock(worker1, 50L, "MEM", blockId, 20L);

    // Send a heartbeat from worker2 saying that it's added blockId.
    List<Long> addedBlocks = ImmutableList.of(blockId);
    mMaster.workerHeartbeat(worker2, ImmutableMap.of("MEM", 0L), NO_BLOCKS,
        ImmutableMap.of("MEM", addedBlocks));

    // The block now has two locations.
    Assert.assertEquals(2, mMaster.getBlockInfo(blockId).getLocations().size());
  }

  @Test
  public void unknownWorkerHeartbeatTriggersRegisterRequest() {
    Command heartBeat = mMaster.workerHeartbeat(0, null, null, null);
    Assert.assertEquals(new Command(CommandType.Register, ImmutableList.<Long>of()), heartBeat);
  }

  @Test
  public void stopTerminatesExecutorService() throws Exception {
    mMaster.stop();
    Assert.assertTrue(mExecutorService.isTerminated());
  }

  @Test
  public void getBlockInfo() throws Exception {
    // Create a worker with a block.
    long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    long blockLength = 20L;
    mMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
        ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_TIERS);
    mMaster.commitBlock(worker1, 50L, "MEM", blockId, blockLength);

    BlockLocation blockLocation = new BlockLocation()
        .setTierAlias("MEM")
        .setWorkerAddress(NET_ADDRESS_1)
        .setWorkerId(worker1);
    BlockInfo expectedBlockInfo = new BlockInfo()
        .setBlockId(1L)
        .setLength(20L)
        .setLocations(ImmutableList.of(blockLocation));
    Assert.assertEquals(expectedBlockInfo, mMaster.getBlockInfo(blockId));
  }
}
