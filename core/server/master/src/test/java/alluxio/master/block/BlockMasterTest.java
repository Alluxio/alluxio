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

import static alluxio.stress.rpc.TierAlias.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.clock.ManualClock;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.AlwaysPrimaryPrimarySelector;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.WorkerState;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.Metric;
import alluxio.proto.meta.Block;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link BlockMaster}.
 */
public class BlockMasterTest {
  public static final long CAPACITY = 20L * 1024 * 1024 * 1024; // 20GB
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(83).setDataPort(84).setWebPort(85);

  private static final List<Long> NO_BLOCKS = ImmutableList.of();
  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATION
      = ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  public static final Map<String, List<String>> LOST_STORAGE =
          ImmutableMap.of(MEM.toString(), ImmutableList.of());
  public static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();
  public static final int BATCH_SIZE = 1000;

  public static final BuildVersion OLD_VERSION = BuildVersion.newBuilder().setVersion("1.0.0")
          .setRevision("foobar").build();
  public static final BuildVersion NEW_VERSION = BuildVersion.newBuilder().setVersion("1.1.0")
          .setRevision("foobaz").build();

  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
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
    // set a large value of PropertyKey.MASTER_LOST_WORKER_DELETION_TIMEOUT_MS
    // to prevent worker to be deleted after it is lost
    Configuration.set(PropertyKey.MASTER_LOST_WORKER_DELETION_TIMEOUT_MS, Integer.MAX_VALUE);
    mRegistry = new MasterRegistry();
    mMetrics = Lists.newArrayList();
    JournalSystem journalSystem = new NoopJournalSystem();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(
        new NoopJournalSystem(), null, new AlwaysPrimaryPrimarySelector()
    );
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
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
  }

  @Test
  public void buildVersion() throws Exception {
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);

    // Sequence to simulate worker upgrade and downgrade,
    // with or without buildVersion in registerWorkerPOptions
    BuildVersion[] buildVersions = new BuildVersion[]{
        null,
        BuildVersion.newBuilder().setVersion("1.0.0")
            .setRevision("foobar").build(),
        BuildVersion.newBuilder().setVersion("1.1.0")
            .setRevision("fizzbuzz").build(),
        null,
    };

    for (BuildVersion bv : buildVersions) {
      RegisterWorkerPOptions options = (bv == null)
          ? RegisterWorkerPOptions.getDefaultInstance()
          : RegisterWorkerPOptions.newBuilder().setBuildVersion(bv).build();

      mBlockMaster.workerRegister(worker1,
          ImmutableList.of(Constants.MEDIUM_MEM),
          ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
          ImmutableMap.of(Constants.MEDIUM_MEM, 10L),
          NO_BLOCKS_ON_LOCATION,
          NO_LOST_STORAGE,
          options);

      BuildVersion actual = mBlockMaster.getWorker(worker1).getBuildVersion();
      assertEquals(bv == null ? "" : bv.getVersion(), actual.getVersion());
      assertEquals(bv == null ? "" : bv.getRevision(), actual.getRevision());
    }
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
  public void detectLostWorker() throws Exception {
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
  public void decommissionWorker() throws Exception {
    // Register a worker.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1,
        ImmutableList.of(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 10L),
        NO_BLOCKS_ON_LOCATION,
        NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Decommission worker
    DecommissionWorkerPOptions options = DecommissionWorkerPOptions.newBuilder()
        .setWorkerHostname(NET_ADDRESS_1.getHost()).setWorkerWebPort(NET_ADDRESS_1.getWebPort())
        .build();
    mBlockMaster.decommissionWorker(options);

    // Make sure the worker is decommissioned.
    int decommissionedCount = mBlockMaster.getDecommissionedWorkerCount();
    int liveCount  = mBlockMaster.getWorkerCount();
    int lostCount = mBlockMaster.getLostWorkerCount();
    assertEquals(1, decommissionedCount);
    assertEquals(0, liveCount);
    assertEquals(0, lostCount);
  }

  @Test
  public void decommissionLostWorker() throws Exception {
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

    // Decommission worker
    DecommissionWorkerPOptions options = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(NET_ADDRESS_1.getHost()).setWorkerWebPort(NET_ADDRESS_1.getWebPort())
            .build();
    mBlockMaster.decommissionWorker(options);

    // Make sure the worker is decommissioned.
    int decommissionedCount = mBlockMaster.getDecommissionedWorkerCount();
    int liveCount  = mBlockMaster.getWorkerCount();
    int lostCount = mBlockMaster.getLostWorkerCount();
    assertEquals(1, decommissionedCount);
    assertEquals(0, liveCount);
    assertEquals(0, lostCount);
  }

  @Test
  public void decommissionCommitUpgradeRegister() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    RegisterWorkerPOptions options = RegisterWorkerPOptions.newBuilder()
            .setBuildVersion(OLD_VERSION).build();
    mBlockMaster.workerRegister(workerId,
            ImmutableList.of(Constants.MEDIUM_MEM),
            ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
            ImmutableMap.of(Constants.MEDIUM_MEM, 0L),
            NO_BLOCKS_ON_LOCATION,
            NO_LOST_STORAGE,
            options);
    List<WorkerInfo> liveWorkerInfo = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerInfo = mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerInfo.size());
    assertEquals(1, allWorkerInfo.size());
    WorkerInfo w = liveWorkerInfo.get(0);
    assertEquals(WorkerState.LIVE.toString(), w.getState());
    assertEquals(OLD_VERSION.getVersion(), w.getVersion());
    assertEquals(OLD_VERSION.getRevision(), w.getRevision());

    // Decommission the worker
    DecommissionWorkerPOptions decomReq = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(NET_ADDRESS_1.getHost()).setWorkerWebPort(NET_ADDRESS_1.getWebPort())
            .setCanRegisterAgain(true)
            .build();
    mBlockMaster.decommissionWorker(decomReq);
    List<WorkerInfo> liveWorkersAfterDecom = mBlockMaster.getWorkerInfoList();
    assertEquals(0, liveWorkersAfterDecom.size());
    List<WorkerInfo> allWorkersAfterDecom =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, allWorkersAfterDecom.size());
    WorkerInfo decomWorker = allWorkersAfterDecom.get(0);
    assertEquals(WorkerState.DECOMMISSIONED.toString(), decomWorker.getState());
    assertEquals(OLD_VERSION.getVersion(), decomWorker.getVersion());
    assertEquals(OLD_VERSION.getRevision(), decomWorker.getRevision());

    // After decommissioned, the worker can still heartbeat to the master
    Map<String, Long> memUsage = ImmutableMap.of(Constants.MEDIUM_MEM, 0L);
    alluxio.grpc.Command heartBeat = mBlockMaster.workerHeartbeat(workerId, null, memUsage,
            NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeat.getCommandType());

    // The leftover operations on the worker can still commit blocks to the master
    long blockId = 1L;
    long blockLength = 100L;
    mBlockMaster.commitBlock(workerId, blockLength, "MEM", "MEM", blockId, blockLength);
    // The block can be found on the master
    BlockInfo blockInfo = mBlockMaster.getBlockInfo(blockId);
    assertNotNull(blockInfo);
    assertEquals(blockInfo.getLength(), blockLength);
    // Although the block can successfully commit, the available locations do not include
    // the decommissioned worker, so clients will not read from that worker for that block
    assertEquals(0, blockInfo.getLocations().size());

    // Heartbeat to the master again, the master does not remove the block incorrectly
    Map<String, Long> memUsageWithBlock = ImmutableMap.of(Constants.MEDIUM_MEM, blockLength);
    List<Long> memBlockList = ImmutableList.of(blockId);
    Block.BlockLocation memTier = Block.BlockLocation.newBuilder()
        .setTier("MEM").setMediumType("MEM").setWorkerId(workerId).build();
    alluxio.grpc.Command heartBeatAgain = mBlockMaster.workerHeartbeat(workerId, null,
        memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
        NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeatAgain.getCommandType());

    // The worker registers again with a higher version
    RegisterWorkerPOptions upgradedWorker = RegisterWorkerPOptions.newBuilder()
            .setBuildVersion(NEW_VERSION).build();
    mBlockMaster.workerRegister(workerId,
        ImmutableList.of(Constants.MEDIUM_MEM),
        memUsageWithBlock,
        memUsageWithBlock,
        ImmutableMap.of(memTier, memBlockList),
        NO_LOST_STORAGE,
        upgradedWorker);
    List<WorkerInfo> liveWorkerAfterRestart = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerAfterRestart =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerAfterRestart.size());
    assertEquals(1, allWorkerAfterRestart.size());
    WorkerInfo restartedWorker = liveWorkerAfterRestart.get(0);
    assertEquals(WorkerState.LIVE.toString(), restartedWorker.getState());
    assertEquals(NEW_VERSION.getVersion(), restartedWorker.getVersion());
    assertEquals(NEW_VERSION.getRevision(), restartedWorker.getRevision());
    MasterWorkerInfo upgradedWorkerInfo = mBlockMaster.getWorker(workerId);
    assertEquals(1, upgradedWorkerInfo.getBlockCount());
    BlockInfo blockInfoCheckAgain = mBlockMaster.getBlockInfo(blockId);
    assertNotNull(blockInfoCheckAgain);
    assertEquals(blockInfoCheckAgain.getLength(), blockLength);
    // The block can be found on the decommissioned worker once the worker registers
    // again after the upgrade
    assertEquals(1, blockInfoCheckAgain.getLocations().size());
    BlockLocation locCheckAgain = blockInfoCheckAgain.getLocations().get(0);
    assertEquals(workerId, locCheckAgain.getWorkerId());

    // Heartbeat to the master again, the master does not remove the block incorrectly
    alluxio.grpc.Command heartBeatAfterUpgrade = mBlockMaster.workerHeartbeat(workerId, null,
        memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
        NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Nothing, heartBeatAfterUpgrade.getCommandType());
  }

  @Test
  public void decommissionCommitUpgradeStreamRegister() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    BlockMasterWorkerServiceHandler handler = new BlockMasterWorkerServiceHandler(mBlockMaster);
    Queue<Throwable> errors =
        streamRegisterWorkerWithVersion(handler, workerId, 0L, ImmutableList.of(), OLD_VERSION);
    assertEquals(0, errors.size());

    List<WorkerInfo> liveWorkerInfo = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerInfo = mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerInfo.size());
    assertEquals(1, allWorkerInfo.size());
    WorkerInfo w = liveWorkerInfo.get(0);
    assertEquals(WorkerState.LIVE.toString(), w.getState());
    assertEquals(OLD_VERSION.getVersion(), w.getVersion());
    assertEquals(OLD_VERSION.getRevision(), w.getRevision());

    // Decommission the worker
    DecommissionWorkerPOptions decomReq = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(NET_ADDRESS_1.getHost()).setWorkerWebPort(NET_ADDRESS_1.getWebPort())
            .setCanRegisterAgain(true)
            .build();
    mBlockMaster.decommissionWorker(decomReq);
    List<WorkerInfo> liveWorkersAfterDecom = mBlockMaster.getWorkerInfoList();
    assertEquals(0, liveWorkersAfterDecom.size());
    List<WorkerInfo> allWorkersAfterDecom =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, allWorkersAfterDecom.size());
    WorkerInfo decomWorker = allWorkersAfterDecom.get(0);
    assertEquals(WorkerState.DECOMMISSIONED.toString(), decomWorker.getState());
    assertEquals(OLD_VERSION.getVersion(), decomWorker.getVersion());
    assertEquals(OLD_VERSION.getRevision(), decomWorker.getRevision());

    // After decommissioned, the worker can still heartbeat to the master
    Map<String, Long> memUsage = ImmutableMap.of(Constants.MEDIUM_MEM, 0L);
    alluxio.grpc.Command heartBeat = mBlockMaster.workerHeartbeat(workerId, null, memUsage,
            NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeat.getCommandType());

    // The leftover operations on the worker can still commit blocks to the master
    long blockId = 1L;
    long blockLength = 100L;
    mBlockMaster.commitBlock(workerId, blockLength, "MEM", "MEM", blockId, blockLength);
    // The block can be found on the master
    BlockInfo blockInfo = mBlockMaster.getBlockInfo(blockId);
    assertNotNull(blockInfo);
    assertEquals(blockInfo.getLength(), blockLength);
    // Although the block can successfully commit, the available locations do not include
    // the decommissioned worker, so clients will not read from that worker for that block
    assertEquals(0, blockInfo.getLocations().size());

    // Heartbeat to the master again, the master does not remove the block incorrectly
    Map<String, Long> memUsageWithBlock = ImmutableMap.of(Constants.MEDIUM_MEM, blockLength);
    List<Long> memBlockList = ImmutableList.of(blockId);
    Block.BlockLocation memTier = Block.BlockLocation.newBuilder()
        .setTier("MEM").setMediumType("MEM").setWorkerId(workerId).build();
    alluxio.grpc.Command heartBeatAgain = mBlockMaster.workerHeartbeat(workerId, null,
            memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
            NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeatAgain.getCommandType());

    // The worker registers again with a higher version
    errors = streamRegisterWorkerWithVersion(handler, workerId, blockLength,
        ImmutableList.of(blockId), NEW_VERSION);
    assertEquals(0, errors.size());
    List<WorkerInfo> liveWorkerAfterRestart = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerAfterRestart =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerAfterRestart.size());
    assertEquals(1, allWorkerAfterRestart.size());
    WorkerInfo restartedWorker = liveWorkerAfterRestart.get(0);
    assertEquals(WorkerState.LIVE.toString(), restartedWorker.getState());
    assertEquals(NEW_VERSION.getVersion(), restartedWorker.getVersion());
    assertEquals(NEW_VERSION.getRevision(), restartedWorker.getRevision());
    MasterWorkerInfo upgradedWorkerInfo = mBlockMaster.getWorker(workerId);
    assertEquals(1, upgradedWorkerInfo.getBlockCount());
    BlockInfo blockInfoCheckAgain = mBlockMaster.getBlockInfo(blockId);
    assertNotNull(blockInfoCheckAgain);
    assertEquals(blockInfoCheckAgain.getLength(), blockLength);
    // The block can be found on the decommissioned worker once the worker registers
    // again after the upgrade
    assertEquals(1, blockInfoCheckAgain.getLocations().size());
    BlockLocation locCheckAgain = blockInfoCheckAgain.getLocations().get(0);
    assertEquals(workerId, locCheckAgain.getWorkerId());

    // Heartbeat to the master again, the master does not remove the block incorrectly
    alluxio.grpc.Command heartBeatAfterUpgrade = mBlockMaster.workerHeartbeat(workerId, null,
        memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
        NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Nothing, heartBeatAfterUpgrade.getCommandType());
  }

  @Test
  public void decommissionRemoveUpgradeStreamRegister() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    BlockMasterWorkerServiceHandler handler = new BlockMasterWorkerServiceHandler(mBlockMaster);

    // Sequence to simulate worker upgrade and downgrade,
    // with or without buildVersion in registerWorkerPOptions
    Queue<Throwable> errors = streamRegisterWorkerWithVersion(handler, workerId, 0L,
        ImmutableList.of(), OLD_VERSION);
    assertEquals(0, errors.size());
    List<WorkerInfo> liveWorkerInfo = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerInfo = mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerInfo.size());
    assertEquals(1, allWorkerInfo.size());
    WorkerInfo w = liveWorkerInfo.get(0);
    assertEquals(WorkerState.LIVE.toString(), w.getState());
    assertEquals(OLD_VERSION.getVersion(), w.getVersion());
    assertEquals(OLD_VERSION.getRevision(), w.getRevision());

    // Prepare a block for removal
    long blockId = 1L;
    long blockLength = 100L;
    mBlockMaster.commitBlock(workerId, blockLength, "MEM", "MEM", blockId, blockLength);

    // Decommission the worker
    DecommissionWorkerPOptions decomReq = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(NET_ADDRESS_1.getHost()).setWorkerWebPort(NET_ADDRESS_1.getWebPort())
            .setCanRegisterAgain(true)
            .build();
    mBlockMaster.decommissionWorker(decomReq);
    List<WorkerInfo> liveWorkersAfterDecom = mBlockMaster.getWorkerInfoList();
    assertEquals(0, liveWorkersAfterDecom.size());
    List<WorkerInfo> allWorkersAfterDecom =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, allWorkersAfterDecom.size());
    WorkerInfo decomWorker = allWorkersAfterDecom.get(0);
    assertEquals(WorkerState.DECOMMISSIONED.toString(), decomWorker.getState());
    assertEquals(OLD_VERSION.getVersion(), decomWorker.getVersion());
    assertEquals(OLD_VERSION.getRevision(), decomWorker.getRevision());

    // After decommissioned, the worker can still heartbeat to the master
    Map<String, Long> memUsage = ImmutableMap.of(Constants.MEDIUM_MEM, 0L);
    alluxio.grpc.Command heartBeat = mBlockMaster.workerHeartbeat(workerId, null, memUsage,
            NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeat.getCommandType());

    // Remove the block from the master and workers
    mBlockMaster.removeBlocks(ImmutableList.of(blockId), true);
    Exception e = assertThrows(BlockInfoException.class, () -> {
      BlockInfo shouldNotExist = mBlockMaster.getBlockInfo(blockId);
    });
    assertTrue(e.getMessage().contains(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(blockId)));

    // Heartbeat to the master again, the master does nothing about the block
    Map<String, Long> memUsageWithBlock = ImmutableMap.of(Constants.MEDIUM_MEM, blockLength);
    List<Long> memBlockList = ImmutableList.of(blockId);
    Block.BlockLocation memTier = Block.BlockLocation.newBuilder()
        .setTier("MEM").setMediumType("MEM").setWorkerId(workerId).build();
    alluxio.grpc.Command heartBeatAgain = mBlockMaster.workerHeartbeat(workerId, null,
        memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
        NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeatAgain.getCommandType());

    // The worker registers again with a higher version
    errors = streamRegisterWorkerWithVersion(handler, workerId, blockLength,
        ImmutableList.of(blockId), NEW_VERSION);
    assertEquals(0, errors.size());
    List<WorkerInfo> liveWorkerAfterRestart = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerAfterRestart =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerAfterRestart.size());
    assertEquals(1, allWorkerAfterRestart.size());
    WorkerInfo restartedWorker = liveWorkerAfterRestart.get(0);
    assertEquals(WorkerState.LIVE.toString(), restartedWorker.getState());
    assertEquals(NEW_VERSION.getVersion(), restartedWorker.getVersion());
    assertEquals(NEW_VERSION.getRevision(), restartedWorker.getRevision());
    MasterWorkerInfo upgradedWorkerInfo = mBlockMaster.getWorker(workerId);
    // The block should not be recognized and therefore the master will want to remove that block
    assertEquals(0, upgradedWorkerInfo.getBlockCount());
    assertEquals(1, upgradedWorkerInfo.getToRemoveBlockCount());

    // Heartbeat to the master again, the master does not remove the block incorrectly
    alluxio.grpc.Command heartBeatAfterUpgrade = mBlockMaster.workerHeartbeat(workerId, null,
            memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
            NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Free, heartBeatAfterUpgrade.getCommandType());
    assertEquals(ImmutableList.of(blockId), heartBeatAfterUpgrade.getDataList());
  }

  @Test
  public void decommissionRemoveUpgradeRegister() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

    // Sequence to simulate worker upgrade and downgrade,
    // with or without buildVersion in registerWorkerPOptions
    RegisterWorkerPOptions options = RegisterWorkerPOptions.newBuilder()
            .setBuildVersion(OLD_VERSION).build();

    mBlockMaster.workerRegister(workerId,
            ImmutableList.of(Constants.MEDIUM_MEM),
            ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
            ImmutableMap.of(Constants.MEDIUM_MEM, 0L),
            NO_BLOCKS_ON_LOCATION,
            NO_LOST_STORAGE,
            options);
    List<WorkerInfo> liveWorkerInfo = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerInfo = mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerInfo.size());
    assertEquals(1, allWorkerInfo.size());
    WorkerInfo w = liveWorkerInfo.get(0);
    assertEquals(WorkerState.LIVE.toString(), w.getState());
    assertEquals(OLD_VERSION.getVersion(), w.getVersion());
    assertEquals(OLD_VERSION.getRevision(), w.getRevision());

    // Prepare a block for removal
    long blockId = 1L;
    long blockLength = 100L;
    mBlockMaster.commitBlock(workerId, blockLength, "MEM", "MEM", blockId, blockLength);

    // Decommission the worker
    DecommissionWorkerPOptions decomReq = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(NET_ADDRESS_1.getHost()).setWorkerWebPort(NET_ADDRESS_1.getWebPort())
            .setCanRegisterAgain(true)
            .build();
    mBlockMaster.decommissionWorker(decomReq);
    List<WorkerInfo> liveWorkersAfterDecom = mBlockMaster.getWorkerInfoList();
    assertEquals(0, liveWorkersAfterDecom.size());
    List<WorkerInfo> allWorkersAfterDecom =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, allWorkersAfterDecom.size());
    WorkerInfo decomWorker = allWorkersAfterDecom.get(0);
    assertEquals(WorkerState.DECOMMISSIONED.toString(), decomWorker.getState());
    assertEquals(OLD_VERSION.getVersion(), decomWorker.getVersion());
    assertEquals(OLD_VERSION.getRevision(), decomWorker.getRevision());

    // After decommissioned, the worker can still heartbeat to the master
    Map<String, Long> memUsage = ImmutableMap.of(Constants.MEDIUM_MEM, 0L);
    alluxio.grpc.Command heartBeat = mBlockMaster.workerHeartbeat(workerId, null, memUsage,
            NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeat.getCommandType());

    // Remove the block from the master and workers
    mBlockMaster.removeBlocks(ImmutableList.of(blockId), true);
    Exception e = assertThrows(BlockInfoException.class, () -> {
      BlockInfo shouldNotExist = mBlockMaster.getBlockInfo(blockId);
    });
    assertTrue(e.getMessage().contains(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(blockId)));

    // Heartbeat to the master again, the master does nothing about the block
    Map<String, Long> memUsageWithBlock = ImmutableMap.of(Constants.MEDIUM_MEM, blockLength);
    List<Long> memBlockList = ImmutableList.of(blockId);
    Block.BlockLocation memTier = Block.BlockLocation.newBuilder()
            .setTier("MEM").setMediumType("MEM").setWorkerId(workerId).build();
    alluxio.grpc.Command heartBeatAgain = mBlockMaster.workerHeartbeat(workerId, null,
            memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
            NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Decommissioned, heartBeatAgain.getCommandType());

    // The worker registers again with a higher version
    RegisterWorkerPOptions upgradedWorker = RegisterWorkerPOptions.newBuilder()
            .setBuildVersion(NEW_VERSION).build();
    mBlockMaster.workerRegister(workerId,
            ImmutableList.of(Constants.MEDIUM_MEM),
            memUsageWithBlock,
            memUsageWithBlock,
            ImmutableMap.of(memTier, memBlockList),
            NO_LOST_STORAGE,
            upgradedWorker);
    List<WorkerInfo> liveWorkerAfterRestart = mBlockMaster.getWorkerInfoList();
    List<WorkerInfo> allWorkerAfterRestart =
        mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(1, liveWorkerAfterRestart.size());
    assertEquals(1, allWorkerAfterRestart.size());
    WorkerInfo restartedWorker = liveWorkerAfterRestart.get(0);
    assertEquals(WorkerState.LIVE.toString(), restartedWorker.getState());
    assertEquals(NEW_VERSION.getVersion(), restartedWorker.getVersion());
    assertEquals(NEW_VERSION.getRevision(), restartedWorker.getRevision());
    MasterWorkerInfo upgradedWorkerInfo = mBlockMaster.getWorker(workerId);
    // The block should not be recognized and therefore the master will want to remove that block
    assertEquals(0, upgradedWorkerInfo.getBlockCount());
    assertEquals(1, upgradedWorkerInfo.getToRemoveBlockCount());

    // Heartbeat to the master again, the master does not remove the block incorrectly
    alluxio.grpc.Command heartBeatAfterUpgrade = mBlockMaster.workerHeartbeat(workerId, null,
            memUsageWithBlock, memBlockList, ImmutableMap.of(memTier, memBlockList),
            NO_LOST_STORAGE, mMetrics);
    assertEquals(CommandType.Free, heartBeatAfterUpgrade.getCommandType());
    assertEquals(ImmutableList.of(blockId), heartBeatAfterUpgrade.getDataList());
  }

  public static Queue<Throwable> streamRegisterWorkerWithVersion(
          BlockMasterWorkerServiceHandler handler,
          long workerId, long blockSize, List<Long> blockList, BuildVersion version) {
    List<RegisterWorkerPRequest> requests = generateRegisterStreamForWorkerWithVersion(
            workerId, blockSize, blockList, version);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(handler, requests, getErrorCapturingResponseObserver(errorQueue));
    return errorQueue;
  }

  public static List<RegisterWorkerPRequest> generateRegisterStreamForWorkerWithVersion(
          long workerId, long blockSize, List<Long> blockList, BuildVersion version) {
    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();
    BlockStoreLocation mem = new BlockStoreLocation("MEM", 0, "MEM");
    blockMap.put(mem, blockList);

    // We just use the RegisterStreamer to generate the batch of requests
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
            workerId, ImmutableList.of("MEM"),
            ImmutableMap.of("MEM", CAPACITY), // capacity
            ImmutableMap.of("MEM", blockSize * blockList.size()), // usage
            blockMap, LOST_STORAGE, EMPTY_CONFIG, version);

    // Get chunks from the RegisterStreamer
    return ImmutableList.copyOf(registerStreamer);
  }

  public static StreamObserver<RegisterWorkerPResponse> getErrorCapturingResponseObserver(
          Queue<Throwable> errorQueue) {
    return new StreamObserver<RegisterWorkerPResponse>() {
      @Override
      public void onNext(RegisterWorkerPResponse response) {}

      @Override
      public void onError(Throwable t) {
        errorQueue.offer(t);
      }

      @Override
      public void onCompleted() {}
    };
  }

  public static void sendStreamToMaster(BlockMasterWorkerServiceHandler handler,
                                        List<RegisterWorkerPRequest> requestChunks,
                                        StreamObserver<RegisterWorkerPResponse> responseObserver) {
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            handler.registerWorkerStream(responseObserver);
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
  }

  @Test
  public void streamRegDecommissionUpgradeStreamReg() throws Exception {
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);

    // Sequence to simulate worker upgrade and downgrade,
    // with or without buildVersion in registerWorkerPOptions
    BuildVersion oldVersion = BuildVersion.newBuilder().setVersion("1.0.0")
            .setRevision("abc").build();
    BuildVersion newVersion = BuildVersion.newBuilder().setVersion("1.1.0")
            .setRevision("def").build();

    BlockMasterWorkerServiceHandler handler = new BlockMasterWorkerServiceHandler(mBlockMaster);
    Queue<Throwable> errors = streamRegisterWorkerWithVersion(handler, worker1, 64 * Constants.MB,
        ImmutableList.of(), oldVersion);
    assertEquals(0, errors.size());

    List<WorkerInfo> availableWorkerList = mBlockMaster.getWorkerInfoList();
    assertEquals(1, availableWorkerList.size());
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(0, mBlockMaster.getLostWorkerCount());
    assertEquals(0, mBlockMaster.getDecommissionedWorkerCount());
    assertEquals(oldVersion.getVersion(), availableWorkerList.get(0).getVersion());
    assertEquals(oldVersion.getRevision(), availableWorkerList.get(0).getRevision());

    // Decommission the worker
    DecommissionWorkerPOptions decomReq = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(NET_ADDRESS_1.getHost()).setWorkerWebPort(NET_ADDRESS_1.getWebPort())
            .setCanRegisterAgain(true)
            .build();
    mBlockMaster.decommissionWorker(decomReq);
    assertEquals(0, mBlockMaster.getWorkerCount());
    assertEquals(0, mBlockMaster.getLostWorkerCount());
    assertEquals(1, mBlockMaster.getDecommissionedWorkerCount());
    List<WorkerInfo> workerReport = mBlockMaster.getWorkerReport(createGetWorkerReportOptions());
    assertEquals(oldVersion.getVersion(), workerReport.get(0).getVersion());
    assertEquals(oldVersion.getRevision(), workerReport.get(0).getRevision());

    // Worker is restarted with a newer version
    errors = streamRegisterWorkerWithVersion(handler, worker1, 64 * Constants.MB,
        ImmutableList.of(), newVersion);
    assertEquals(0, errors.size());
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(0, mBlockMaster.getLostWorkerCount());
    assertEquals(0, mBlockMaster.getDecommissionedWorkerCount());
    List<WorkerInfo> availableWorkerListNow = mBlockMaster.getWorkerInfoList();
    assertEquals(newVersion.getVersion(), availableWorkerListNow.get(0).getVersion());
    assertEquals(newVersion.getRevision(), availableWorkerListNow.get(0).getRevision());
  }

  private GetWorkerReportOptions createGetWorkerReportOptions() {
    GetWorkerReportOptions getReportOptions = GetWorkerReportOptions.defaults();
    getReportOptions.setFieldRange(GetWorkerReportOptions.WorkerInfoField.ALL);
    getReportOptions.setWorkerRange(GetWorkerReportOptions.WorkerRange.ALL);
    return getReportOptions;
  }

  @Test
  public void autoDeleteTimeoutWorker() throws Exception {

    // In default configuration the lost worker will never be deleted. So set a short timeout
    Configuration.set(PropertyKey.MASTER_LOST_WORKER_DELETION_TIMEOUT_MS, 1000);
    // Register a worker.
    long worker1 = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    mBlockMaster.workerRegister(worker1,
        ImmutableList.of(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        ImmutableMap.of(Constants.MEDIUM_MEM, 10L),
        NO_BLOCKS_ON_LOCATION,
        NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Advance the block master's clock by an hour so that worker can be deleted.
    mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

    // Run the lost worker detector.
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);

    // Make sure the worker has been deleted.
    List<WorkerInfo> info = mBlockMaster.getLostWorkersInfoList();
    assertEquals(0, mBlockMaster.getLostWorkersInfoList().size());
    assertThrows(NotFoundException.class, () -> mBlockMaster.getWorker(worker1));
    assertEquals(0, mBlockMaster.getWorkerCount());
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
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<Long> orphanedBlocks = Arrays.asList(1L, 2L);
    Map<String, Long> memUsage = ImmutableMap.of(Constants.MEDIUM_MEM, 10L);

    Block.BlockLocation blockLoc = Block.BlockLocation.newBuilder()
        .setWorkerId(workerId).setTier(Constants.MEDIUM_MEM)
        .setMediumType(Constants.MEDIUM_MEM).build();
    mBlockMaster.workerRegister(workerId, Arrays.asList(Constants.MEDIUM_MEM),
        ImmutableMap.of(Constants.MEDIUM_MEM, 100L),
        memUsage, ImmutableMap.of(blockLoc, orphanedBlocks), NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());

    // Check that the worker heartbeat tells the worker to remove the blocks.
    alluxio.grpc.Command heartBeat = mBlockMaster.workerHeartbeat(workerId, null,
        memUsage, NO_BLOCKS, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE, mMetrics);
    assertEquals(orphanedBlocks,
        heartBeat.getDataList().stream().sorted().collect(Collectors.toList()));
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
    Block.BlockLocation blockOnWorker2 = Block.BlockLocation.newBuilder()
        .setWorkerId(worker2).setTier(Constants.MEDIUM_MEM)
        .setMediumType(Constants.MEDIUM_MEM).build();
    mBlockMaster.workerHeartbeat(worker2, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L), NO_BLOCKS,
        ImmutableMap.of(blockOnWorker2, addedBlocks),
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

  @Test
  public void getNewContainerId() throws Exception {
    final int total = 10_000;
    long containerIdReservationSize = Configuration.getInt(
        PropertyKey.MASTER_CONTAINER_ID_RESERVATION_SIZE);

    final int parallelNum = 5;
    ExecutorService containerIdFetcher = Executors.newFixedThreadPool(parallelNum);
    AtomicInteger times = new AtomicInteger(0);
    assertEquals(0L, mBlockMaster.getNewContainerId());

    CyclicBarrier barrier = new CyclicBarrier(parallelNum);

    for (int count = 0; count < parallelNum; count++) {
      containerIdFetcher.submit(() -> {
        try {
          barrier.await();
          while (times.incrementAndGet() < total) {
            mBlockMaster.getNewContainerId();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    containerIdFetcher.shutdown();
    containerIdFetcher.awaitTermination(6, TimeUnit.SECONDS);
    mBlockMaster.close();

    long journaledNextContainerId = mBlockMaster.getJournaledNextContainerId();

    assertTrue(journaledNextContainerId >= total);
    assertTrue(journaledNextContainerId <= total + containerIdReservationSize);
  }

  @Test
  public void stop() throws Exception {
    mRegistry.stop();
    assertTrue(mExecutorService.isShutdown());
    assertTrue(mExecutorService.isTerminated());
  }
}
