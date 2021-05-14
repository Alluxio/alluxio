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

import alluxio.Server;
import alluxio.StorageTierAssoc;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.clock.ManualClock;
import alluxio.exception.BlockInfoException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcService;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.ServiceType;
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.SafeModeManager;
import alluxio.master.TestSafeModeManager;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.Metric;
import alluxio.proto.journal.Journal;
import alluxio.proto.meta.Block;
import alluxio.resource.CloseableIterator;
import alluxio.util.CommonUtils;
import alluxio.util.SleepUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.wire.Address;
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

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Unit tests for {@link BlockMaster}.
 */
public class BlockMasterTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(83).setDataPort(84).setWebPort(85);

  private static final List<Long> NO_BLOCKS = ImmutableList.of();
  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATION
      = ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final Block.BlockLocation BLOCK_LOCATION = Block.BlockLocation.newBuilder()
      .setTier(Constants.MEDIUM_MEM).setMediumType(Constants.MEDIUM_MEM).build();

  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private ExecutorService mClientExecutorService;
  private SafeModeManager mSafeModeManager;
  private long mStartTimeMs;
  private int mPort;
  private MetricsMaster mMetricsMaster;
  private List<Metric> mMetrics;

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
    mStartTimeMs = System.currentTimeMillis();
    mPort = ServerConfiguration.getInt(PropertyKey.MASTER_RPC_PORT);
    mMetrics = Lists.newArrayList();
    JournalSystem journalSystem = new NoopJournalSystem();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mClientExecutorService = Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mBlockMaster = new DefaultBlockMaster(mMetricsMaster, masterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);
  }

  /**
   * Stops the master after a test ran.
   */
  @After
  public void after() throws Exception {
    mRegistry.stop();

    // When the registry is stopped, the BlockMaster will stop the given ExecutorService
    // We need to manually shutdown this client thread pool
    mClientExecutorService.shutdown();
  }

  @Test
  public void countBytes() throws Exception {
    // Register two workers
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    List<String> tiers = Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD);
    Map<String, Long> worker1TotalBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, 10L, Constants.MEDIUM_SSD, 20L);
    Map<String, Long> worker2TotalBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, 1000L, Constants.MEDIUM_SSD, 2000L);
    Map<String, Long> worker1UsedBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, 1L, Constants.MEDIUM_SSD, 2L);
    Map<String, Long> worker2UsedBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L, Constants.MEDIUM_SSD, 200L);
    mBlockMaster.workerRegister(worker1, tiers, worker1TotalBytesOnTiers, worker1UsedBytesOnTiers,
        NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, RegisterWorkerPOptions.getDefaultInstance());
    mBlockMaster.workerRegister(worker2, tiers, worker2TotalBytesOnTiers, worker2UsedBytesOnTiers,
        NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, RegisterWorkerPOptions.getDefaultInstance());

    // Check that byte counts are summed correctly.
    assertEquals(3030, mBlockMaster.getCapacityBytes());
    assertEquals(303L, mBlockMaster.getUsedBytes());
    assertEquals(ImmutableMap.of(Constants.MEDIUM_MEM, 1010L, Constants.MEDIUM_SSD, 2020L),
        mBlockMaster.getTotalBytesOnTiers());
    assertEquals(ImmutableMap.of(Constants.MEDIUM_MEM, 101L, Constants.MEDIUM_SSD, 202L),
        mBlockMaster.getUsedBytesOnTiers());
  }

  @Test
  public void detectLostWorkers() throws Exception {
    // Register a worker.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1,
        ImmutableList.of(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 10L),
        NO_BLOCKS_ON_LOCATION,
        NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

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
        ImmutableList.of(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 10L),
        NO_BLOCKS_ON_LOCATION,
        NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Advance the block master's clock by an hour so that the worker appears lost.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);

    // Reregister the worker using its original worker id.
    mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1,
        ImmutableList.of(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 10L),
        NO_BLOCKS_ON_LOCATION,
        NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Check that there are no longer any lost workers and there is a live worker.
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(0, mBlockMaster.getLostWorkersInfoList().size());
  }

  @Test
  public void removeBlockTellsWorkersToRemoveTheBlock() throws Exception {
    // Create a worker with a block.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    mBlockMaster.workerRegister(worker1, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());
    mBlockMaster.commitBlock(worker1, 50L,
        Constants.MEDIUM_MEM, Constants.MEDIUM_MEM, blockId, 20L);

    // Remove the block
    mBlockMaster.removeBlocks(Arrays.asList(1L), /*delete=*/false);

    // Check that the worker heartbeat tells the worker to remove the block.
    Map<String, Long> memUsage = ImmutableMap.of(Constants.MEDIUM_MEM, 0L);
    alluxio.grpc.Command heartBeat = mBlockMaster.workerHeartbeat(worker1, null, memUsage,
        NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertEquals(ImmutableList.of(1L), heartBeat.getDataList());
  }

  @Test
  public void registerCleansUpOrphanedBlocks() throws Exception {
    // Create a worker with unknown blocks.
    long worker = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<Long> orphanedBlocks = Arrays.asList(1L, 2L);
    Map<String, Long> memUsage = ImmutableMap.of(Constants.MEDIUM_MEM, 10L);
    mBlockMaster.workerRegister(worker, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        memUsage, ImmutableMap.of(BLOCK_LOCATION, orphanedBlocks), NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Check that the worker heartbeat tells the worker to remove the blocks.
    alluxio.grpc.Command heartBeat = mBlockMaster.workerHeartbeat(worker, null,
        memUsage, NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertEquals(orphanedBlocks, heartBeat.getDataList());
  }

  @Test
  public void workerHeartbeatUpdatesMemoryCount() throws Exception {
    // Create a worker.
    long worker = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    Map<String, Long> initialUsedBytesOnTiers = ImmutableMap.of(Constants.MEDIUM_MEM, 50L);
    mBlockMaster.workerRegister(worker, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        initialUsedBytesOnTiers, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Update used bytes with a worker heartbeat.
    Map<String, Long> newUsedBytesOnTiers = ImmutableMap.of(Constants.MEDIUM_MEM, 50L);
    mBlockMaster.workerHeartbeat(worker, null, newUsedBytesOnTiers,
        NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);

    WorkerInfo workerInfo = Iterables.getOnlyElement(mBlockMaster.getWorkerInfoList());
    assertEquals(50, workerInfo.getUsedBytes());
  }

  @Test
  public void workerHeartbeatUpdatesRemovedBlocks() throws Exception {
    // Create a worker.
    long worker = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());
    long blockId = 1L;
    mBlockMaster.commitBlock(worker, 50L, Constants.MEDIUM_MEM,
        Constants.MEDIUM_MEM, blockId, 20L);

    // Indicate that blockId is removed on the worker.
    mBlockMaster.workerHeartbeat(worker, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L),
        ImmutableList.of(blockId), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertTrue(mBlockMaster.getBlockInfo(blockId).getLocations().isEmpty());
  }

  @Test
  public void workerHeartbeatUpdatesAddedBlocks() throws Exception {
    // Create two workers.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    mBlockMaster.workerRegister(worker2, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Commit blockId to worker1.
    long blockId = 1L;
    mBlockMaster.commitBlock(worker1, 50L, Constants.MEDIUM_MEM,
        Constants.MEDIUM_MEM, blockId, 20L);

    // Send a heartbeat from worker2 saying that it's added blockId.
    List<Long> addedBlocks = ImmutableList.of(blockId);
    mBlockMaster.workerHeartbeat(worker2, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS,
        ImmutableMap.of(BLOCK_LOCATION, addedBlocks),
        NO_LOST_STORAGE, mMetrics);

    // The block now has two locations.
    assertEquals(2, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  @Test
  public void workerHeartbeatUpdatesLostStorage() throws Exception {
    // Create two workers.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1, Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L, Constants.MEDIUM_SSD, 200L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L, Constants.MEDIUM_SSD, 0L),
        NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    mBlockMaster.workerRegister(worker2, Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_HDD),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L, Constants.MEDIUM_HDD, 300L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L, Constants.MEDIUM_HDD, 0L),
        NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    Map<String, StorageList> lostStorageOnWorker1 = new HashMap<>();
    lostStorageOnWorker1.put(Constants.MEDIUM_SSD, StorageList.newBuilder()
        .addAllStorage(Arrays.asList("/ssd/one", "/ssd/two")).build());
    Map<String, StorageList> lostStorageOnWorker2 = new HashMap<>();
    lostStorageOnWorker2.put(Constants.MEDIUM_HDD,
        StorageList.newBuilder().addStorage("/hdd/one").build());

    mBlockMaster.workerHeartbeat(worker1,
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L, Constants.MEDIUM_SSD, 0L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L, Constants.MEDIUM_SSD, 0L), NO_BLOCKS,
        NO_BLOCKS_ON_LOCATION, lostStorageOnWorker1, mMetrics);
    mBlockMaster.workerHeartbeat(worker2,
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L, Constants.MEDIUM_HDD, 200L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L, Constants.MEDIUM_HDD, 0L), NO_BLOCKS,
        NO_BLOCKS_ON_LOCATION, lostStorageOnWorker2, mMetrics);

    // Two workers have lost storage paths
    assertEquals(2, mBlockMaster.getWorkerLostStorage().size());
    int lostStorageNum = 0;
    for (WorkerLostStorageInfo info : mBlockMaster.getWorkerLostStorage()) {
      for (StorageList list : info.getLostStorageMap().values()) {
        lostStorageNum += list.getStorageList().size();
      }
    }
    assertEquals(3, lostStorageNum);
  }

  @Test
  public void unknownWorkerHeartbeatTriggersRegisterRequest() {
    Command heartBeat = mBlockMaster.workerHeartbeat(0, null, null, null, null, null, mMetrics);
    assertEquals(Command.newBuilder().setCommandType(CommandType.Register).build(), heartBeat);
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
    mBlockMaster.workerRegister(worker1, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());
    mBlockMaster.commitBlock(worker1, 50L, Constants.MEDIUM_MEM,
        Constants.MEDIUM_MEM, blockId, blockLength);

    BlockLocation blockLocation = new BlockLocation()
        .setTierAlias(Constants.MEDIUM_MEM)
        .setWorkerAddress(NET_ADDRESS_1)
        .setWorkerId(worker1)
        .setMediumType(Constants.MEDIUM_MEM);
    BlockInfo expectedBlockInfo = new BlockInfo()
        .setBlockId(1L)
        .setLength(20L)
        .setLocations(ImmutableList.of(blockLocation));
    assertEquals(expectedBlockInfo, mBlockMaster.getBlockInfo(blockId));
  }

  /**
   * RW contention
   * Concurrent commit and readers
   * Signal in commit and the readers inquire the state
   * */
  @Test
  public void concurrentCommitWithReaders() throws Exception {
    JournalSystem journalSystem = new NoopJournalSystem();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();

    CountDownLatch readerLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), readerLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    long blockLength = 49L;
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());

    // clients keep reading the blockInfo
    // Each client is constantly checking the worker status
    // This thread count is intentionally larger than the client thread pool
    // In the hope that even if the first batch of clients all read the state before commit really happens
    // The following batch will capture the state after the commit
    int threadCount = 20;
    Queue<Throwable> uncaughtThrowables = new ConcurrentLinkedQueue<>();
    CountDownLatch allClientFinished = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      int finalI = i;
      mClientExecutorService.submit(() -> {
        try {
          // Wait until the commit is already running
          readerLatch.await();

          // If the block is not committed yet, a BlockInfoException will be thrown
          BlockInfo blockInfo = testMaster.getBlockInfo(blockId);
          List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());

          BlockLocation blockLocation = new BlockLocation()
                  .setTierAlias("MEM")
                  .setWorkerAddress(NET_ADDRESS_1)
                  .setWorkerId(worker1)
                  .setMediumType("MEM");
          BlockInfo expectedBlockInfo = new BlockInfo()
                  .setBlockId(blockId)
                  .setLength(blockLength)
                  .setLocations(ImmutableList.of(blockLocation));
          assertEquals(expectedBlockInfo, blockInfo);
          assertEquals(1, workerInfoList.size());
          WorkerInfo worker = workerInfoList.get(0);
          assertEquals(49L, worker.getUsedBytes());
        } catch (BlockInfoException e) {
          // The reader came in before the writer started the commit
          try {
            List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
            assertEquals(1, workerInfoList.size());
            WorkerInfo worker = workerInfoList.get(0);
            // We may just see the result before or after the commit
            // But other values should be illegal
            assertTrue(49L == worker.getUsedBytes() || 100L == worker.getUsedBytes());
          } catch (Throwable t) {
            uncaughtThrowables.add(t);
          }
        } catch (Throwable t) {
          uncaughtThrowables.add(t);
        } finally {
          allClientFinished.countDown();
        }
      });
    }

    // Sometime in the middle, a block is committed
    testMaster.commitBlock(worker1, 49L, "MEM", "MEM", blockId, blockLength);

    allClientFinished.await();
    // If any assertion failed, the failed assertion will throw an AssertError
    while (!uncaughtThrowables.isEmpty()) {
      Throwable t = uncaughtThrowables.poll();
      t.printStackTrace();
    }
    assertEquals(0, uncaughtThrowables.size());
  }

  /**
   * RW contention
   * Concurrent remove and readers
   * Signal in remove and the readers inquire the state
   * */

  /**
   * WW contention
   * Write operations are:
   * 1. commit
   * 2. remove
   * 3. workerRegister
   * 4. workerHeartbeat
   *
   * It's hard to trigger a signal in workerRegister and workerHeartbeat
   *
   * Test W1 race condition with W2 where W1 will send a signal in the middle of run and trigger W2
   * W1 is commit/remove
   * W2 is commit/remove/workerRegister/workerHeartbeat
   *
   * When W1 is operating on block B, if W2 is commit/remove:
   * 1. W2 is on the same block
   * 2. W2 is on a different block
   *
   * When W1 is operating on block B, if W2 is workerRegister/workerHeartbeat,
   * the options are:
   * Opt1: W2 may be from the same worker or a different worker
   * Opt2: W2 may contain the same block or not
   * */

  @Test
  public void stop() throws Exception {
    mRegistry.stop();
    assertTrue(mExecutorService.isShutdown());
    assertTrue(mExecutorService.isTerminated());

    assertTrue(mClientExecutorService.isShutdown());
    assertTrue(mClientExecutorService.isTerminated());
  }
}
