package alluxio.client.fs;

import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.clock.ManualClock;
import alluxio.clock.SystemClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockInfoException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetWorkerIdPResponse;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.block.BlockMasterTestUtils;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterWorkerServiceHandler;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.meta.Block;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.stress.rpc.TierAlias.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Integration tests for the server-side logic for the register stream.
 */
public class BlockMasterRegisterStreamIntegrationTest {
  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private MetricsMaster mMetricsMaster;
  private BlockMasterWorkerServiceHandler mHandler;

  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
          .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
          .setRpcPort(83).setDataPort(84).setWebPort(85);

  public static final Map<String, List<String>> LOST_STORAGE =
          ImmutableMap.of(MEM.toString(), ImmutableList.of());
  public static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();
  public static final long BLOCK_SIZE = ServerConfiguration.global().getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);

  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATION =
          ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final int CONCURRENT_CLIENT_COUNT = 20;
  private static final long BLOCK1_ID = 1L;
  private static final long BLOCK1_LENGTH = 49L;
  private static final long BLOCK2_ID = 2L;
  private static final long BLOCK2_LENGTH = 59L;
  private static final Map<String, Long> MEM_CAPACITY = ImmutableMap.of("MEM", 100L);
  private static final Map<String, Long> MEM_USAGE_EMPTY = ImmutableMap.of("MEM", 0L);
  private static final Command FREE_BLOCK1_CMD = Command.newBuilder()
          .setCommandType(CommandType.Free).addData(1).build();
  private static final Command EMPTY_CMD = Command.newBuilder()
          .setCommandType(CommandType.Nothing).build();

  private static final int MASTER_WORKER_TIMEOUT = 1_000_000;

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set the config properties
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, true);
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE, 1000);
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_TIMEOUT_MS, MASTER_WORKER_TIMEOUT);

    // TODO(jiacheng): use a manual clock to better control
    ServerConfiguration.set(PropertyKey.MASTER_REGISTER_WORKER_STREAM_TIMEOUT, "1s");

    mRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mClock = new ManualClock();

    mExecutorService =
            Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mBlockMaster = new DefaultBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);
    mHandler = new BlockMasterWorkerServiceHandler(mBlockMaster);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  public long getWorkerId(WorkerNetAddress address) throws Exception {
    long workerId = mBlockMaster.getWorkerId(address);
    System.out.println("Worker id " + workerId);
    return workerId;
  }

  public void prepareBlocksOnMaster(Map<BlockStoreLocation, List<Long>> blockMap) throws UnavailableException {
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockMap.entrySet()) {
      BlockStoreLocation loc = entry.getKey();
      for (long blockId : entry.getValue()) {
        mBlockMaster.commitBlockInUFS(blockId, BLOCK_SIZE);
      }
    }
  }

  public void prepareBLocksOnMaster(Collection<Long> blockIds) throws UnavailableException {
    for (long id : blockIds) {
      mBlockMaster.commitBlockInUFS(id, BLOCK_SIZE);
    }
  }


  public List<RegisterWorkerPRequest> generateRegisterStreamForEmptyWorker(long workerId) throws Exception {
    String tierConfig = "";
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    prepareBlocksOnMaster(blockMap);
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
            workerId, ImmutableList.of("MEM"), MEM_CAPACITY, MEM_USAGE_EMPTY, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // For an empty worker there is only 1 request
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    assertEquals(1, requestChunks.size());

    return requestChunks;
  }

  public List<RegisterWorkerPRequest> generateRegisterStreamForWorker(long workerId) throws Exception {
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // We just use the RegisterStreamer to generate the batch of requests
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    int expectedBatchCount = (int) Math.ceil((100+200+300+1000+1500+2000)/(double)1000);
    assertEquals(expectedBatchCount, requestChunks.size());

    return requestChunks;
  }

  /**
   * Tests below cover the most normal cases.
   */
  @Test
  public void registerEmptyWorkerStream() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForEmptyWorker(workerId);

    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getNoopResponseObserver());

    // Manually feed the chunks to the master side
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // Verify the worker is registered
    assertEquals(0, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  public void registerWorkerStream() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);

    StreamObserver<RegisterWorkerPRequest> requestObserver =
        mHandler.registerWorkerStream(RegisterStreamTestUtils.getNoopResponseObserver());

    // Manually feed the chunks to the master side
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // Verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  public void registerLostWorker() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

    // TODO(jiacheng): simpler way to prepare blocks
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Register the worker on the master
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getNoopResponseObserver());

    // We just use the RegisterStreamer to generate the batch of requests
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    int expectedBatchCount = (int) Math.ceil((100+200+300+1000+1500+2000)/(double)1000);
    assertEquals(expectedBatchCount, requestChunks.size());

    // Manually feed the chunks to the master side
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // The worker has lost heartbeat and been forgotten
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    long newTimeMs = worker.getLastUpdatedTimeMs() + MASTER_WORKER_TIMEOUT + 1;
    mClock.setTimeMs(newTimeMs);
    DefaultBlockMaster.LostWorkerDetectionHeartbeatExecutor lostWorkerDetector =
        ((DefaultBlockMaster) mBlockMaster).new LostWorkerDetectionHeartbeatExecutor();
    lostWorkerDetector.heartbeat();

    // Verify the worker has been forgotten
    assertEquals(0, mBlockMaster.getWorkerCount());

    // Register again
    StreamObserver<RegisterWorkerPRequest> newRequestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getNoopResponseObserver());

    // Use the same requests to register
    for (RegisterWorkerPRequest chunk : requestChunks) {
      newRequestObserver.onNext(chunk);
    }
    newRequestObserver.onCompleted();
    System.out.println("Re-register stream completed on client side");

    // Verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  private void sendStreamToMaster(List<RegisterWorkerPRequest> requestChunks, StreamObserver<RegisterWorkerPResponse> responseObserver) {
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(responseObserver);
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    System.out.println("All requests have been sent from the client side");
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");
  }

  private void sendStreamToMasterAndSignal(
      List<RegisterWorkerPRequest> requestChunks,
      StreamObserver<RegisterWorkerPResponse> responseObserver,
      CountDownLatch latch) {
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(responseObserver);
    for (int i = 0; i < requestChunks.size(); i++) {
      CommonUtils.sleepMs(100);
      RegisterWorkerPRequest chunk = requestChunks.get(i);
      requestObserver.onNext(chunk);
      // Signal after the 1st request has been sent
      if (i == 0) {
        System.out.println("Signal");
        latch.countDown();
      }
    }
    System.out.println("All requests have been sent from the client side");
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");
  }

  @Test
  // The worker has not yet been forgotten by the master but attempts to register again.
  // This can happen when a worker process is restarted.
  public void registerExistingWorker() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);

    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    // Register again
    System.out.println("Re-register now");
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    // Verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  public void registerExistingWorkerBlocksLost() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);

    // TODO(jiacheng): simpler way to prepare blocks
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    // Register the worker on the master
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getNoopResponseObserver());

    // We just use the RegisterStreamer to generate the batch of requests
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    int expectedBatchCount = (int) Math.ceil((100+200+300+1000+1500+2000)/(double)1000);
    assertEquals(expectedBatchCount, requestChunks.size());

    // Manually feed the chunks to the master side
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // Register again
    StreamObserver<RegisterWorkerPRequest> newRequestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getNoopResponseObserver());

    // Regenerate the requests
    Set<Long> lostBlocks = removeSomeBlocks(blockMap);
    System.out.println(lostBlocks + " blocks went lost");
    RegisterStreamer newRegisterStreamer = new RegisterStreamer(null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    List<RegisterWorkerPRequest> newRequestChunks = ImmutableList.copyOf(newRegisterStreamer);
    int newExpectedBatchCount = (int) Math.ceil((100+200+300+1000+1500+2000-lostBlocks.size())/(double)1000);
    assertEquals(newExpectedBatchCount, newRequestChunks.size());

    // Re-register with the new requests
    for (RegisterWorkerPRequest chunk : newRequestChunks) {
      newRequestObserver.onNext(chunk);
    }
    newRequestObserver.onCompleted();
    System.out.println("Re-register stream completed on client side");

    // Verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000-lostBlocks.size(), mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // The update is received during the registration so no command to send to the worker
    Command command = mBlockMaster.workerHeartbeat(workerId,
        mCapacityMap,
        mUsedMap,
        // list of removed blockIds
        ImmutableList.of(),
        ImmutableMap.of(),
        NO_LOST_STORAGE,
        ImmutableList.of());
    assertEquals(EMPTY_CMD, command);

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  private int findBlockCount(RegisterWorkerPRequest request) {
    List<LocationBlockIdListEntry> entries = request.getCurrentBlocksList();
    int totalCount = 0;
    for (LocationBlockIdListEntry entry : entries) {
      totalCount += entry.getValue().getBlockIdCount();
    }
    return totalCount;
  }

  private Set<Long> findBlockIds(RegisterWorkerPRequest request) {
    Set<Long> blockIds = new HashSet<>();
    List<LocationBlockIdListEntry> entries = request.getCurrentBlocksList();
    for (LocationBlockIdListEntry entry : entries) {
      blockIds.addAll(entry.getValue().getBlockIdList());
    }
    return blockIds;
  }

  // Removes blocks from the current worker blocks.
  // Returns the set of blocks that are removed.
  private Set<Long> removeSomeBlocks(Map<BlockStoreLocation, List<Long>> blockMap) {
    Set<Long> toRemove = new HashSet<>();
    // 1 block goes missing from each location
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockMap.entrySet()) {
      List<Long> blocks = entry.getValue();
      if (blocks.isEmpty()) {
        continue;
      }
      toRemove.add(blocks.get(0));
      blocks.remove(0);
    }
    return toRemove;
  }

  // Add some blocks to the current worker blocks.
  // Returns the set of blocks that are added.
  private Set<Long> addSomeBlocks(Map<BlockStoreLocation, List<Long>> blockMap) {
    long blockId = 100L;
    Set<Long> added = new HashSet<>();
    // 1 block is added into each location
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockMap.entrySet()) {
      List<Long> blocks = entry.getValue();
      added.add(blockId);
      blocks.add(blockId);
      blockId++;
    }
    return added;
  }

  @Test
  public void registerExistingWorkerBlocksAdded() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    // Get chunks from the RegisterStreamer
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    String tierConfig = "100,200,300;1000,1500;2000";
    // Generate block IDs in the same way but add some more
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));
    Set<Long> addedBlocks = addSomeBlocks(blockMap);

    // Make the master accept these blocks
    prepareBLocksOnMaster(addedBlocks);
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    System.out.println(addedBlocks + " blocks are added");
    RegisterStreamer newRegisterStreamer = new RegisterStreamer(null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    List<RegisterWorkerPRequest> newRequestChunks = ImmutableList.copyOf(newRegisterStreamer);
    int newExpectedBatchCount = (int) Math.ceil((100+200+300+1000+1500+2000+addedBlocks.size())/(double)1000);
    assertEquals(newExpectedBatchCount, newRequestChunks.size());

    System.out.println("Re-registering with added blocks");
    sendStreamToMaster(newRequestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    // Verify the worker is registered
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(100+200+300+1000+1500+2000+addedBlocks.size(), mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // No command from the master because the update is received during registration
    Command command = mBlockMaster.workerHeartbeat(workerId,
            mCapacityMap,
            mUsedMap,
            // list of removed blockIds
            ImmutableList.of(),
            ImmutableMap.of(),
            NO_LOST_STORAGE,
            ImmutableList.of());
    System.out.println("Command is " + command);
    assertEquals(EMPTY_CMD, command);

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }


  // Verify the worker is writable by trying to commit a block in it
  private void verifyWorkerWritable(long workerId) throws Exception {
    // First see the worker usage
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    long used = worker.getUsedBytes();
    // Commit a new block to the worker
    long newBlockId = 999L;
    long newBlockSize = 1024L;

    mBlockMaster.commitBlock(workerId, used + newBlockSize,
            "MEM", "MEM", newBlockId, newBlockSize);

    List<WorkerInfo> workers = mBlockMaster.getWorkerInfoList();
    WorkerInfo workerInfo = BlockMasterTestUtils.findWorkerInfo(workers, workerId);
    BlockMasterTestUtils.verifyBlockOnWorkers(mBlockMaster, newBlockId, newBlockSize, ImmutableList.of(workerInfo));
  }

  private static List<String> getTierAliases(Map<TierAlias, List<Integer>> tierConfig) {
    return tierConfig.keySet().stream().map(TierAlias::toString).collect(Collectors.toList());
  }

  // TODO(jiacheng): improve this
  public Map<TierAlias, List<Integer>> convert(String tiersConfig) {
    String[] tiers = tiersConfig.split(";");
    if (tiers.length == 1 && "".equals(tiers[0])) {
      return ImmutableMap.of();
    }
    int length = Math.min(tiers.length, TierAlias.values().length);
    ImmutableMap.Builder<TierAlias, List<Integer>> builder = new ImmutableMap.Builder<>();
    for (int i = 0; i < length; i++) {
      builder.put(
              TierAlias.SORTED.get(i),
              Arrays.stream(tiers[i].split(","))
                      .map(Integer::parseInt)
                      .collect(Collectors.toList()));
    }
    return builder.build();
  }

  public long selectABlock(RegisterWorkerPRequest request) {
    List<LocationBlockIdListEntry> entryList = request.getCurrentBlocksList();
    if (entryList.size() > 0) {
      LocationBlockIdListEntry entry = entryList.get(0);
      List<Long> blocks = entry.getValue().getBlockIdList();
      if (blocks.size() > 0) {
        return blocks.get(0);
      }
    }
    return -1;
  }

  public void registerEmptyWorker(long workerId, WorkerNetAddress address) throws Exception {
    mBlockMaster.workerRegister(workerId, Arrays.asList("MEM"), MEM_CAPACITY,
            MEM_USAGE_EMPTY, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
  }

  /**
   * Tests below cover various failure cases.
   */
  // TODO(jiacheng): worker hangs, session recycled, worker unlocked
  @Test
  public void hangingworker() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    WorkerNetAddress address = new WorkerNetAddress().setWebPort(0).setRpcPort(0).setDataPort(0).setHost(hostname);

    long workerId = getWorkerId(address);

    List<String> mTierAliases;
    Map<String, Long> mCapacityMap;
    Map<String, Long> mUsedMap;
    String tierConfig = "100,200,300;1000,1500;2000";
    mTierAliases = getTierAliases(convert(tierConfig));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // Prepare the blocks on the master
    prepareBlocksOnMaster(blockMap);

    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getNoopResponseObserver());

    // Send the chunks with the requestObserver
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);

    // Feed the chunks into the requestObserver
    for (RegisterWorkerPRequest chunk : requestChunks) {
      // TODO(jiacheng): rate limit this? ACK until the next send?
      //  The 2nd chunk will receive an error, then keep sending should get rejected
      requestObserver.onNext(chunk);



      // TODO(jiacheng): use the manual clock
      System.out.println("Sleep 5s on the client side, should trigger timeout");
      CommonUtils.sleepMs(5000);
    }
    // TODO(jiacheng): This should be rejected too
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    // verify the worker is registered
    assertEquals(100+200+300+1000+1500+2000, mBlockMaster.getWorker(workerId).getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  @Test
  public void workerSendsErrorOnStart() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    // Instead of sending requests to the master, the worker is interrupted
    // around the beginning of the stream. The error propagated to the master.
    Exception e = new InterruptedException("Worker is interrupted");
    requestObserver.onError(e);
    System.out.println("Worker sends error before the 1st request");

    // Verify that after the requestObserver.onError(), no requests
    // shall be accepted by the master.
    // TODO(jiacheng): onError is the 1st thing received, the context was not open when it is closed
    //  when the request comes in, context==null so it is created as open!
//    for (RegisterWorkerPRequest chunk : requestChunks) {
//      requestObserver.onNext(chunk);
//    }
//    requestObserver.onCompleted();
//    assertEquals(requestChunks.size() + 1, errorQueue.size());

    // verify the worker is not registered
    assertEquals(0, mBlockMaster.getWorkerCount());

    verifyWorkerCanReregister(workerId, requestChunks, 100+200+300+1000+1500+2000);
  }

  // Verifies the worker can re-register to the master
  private void verifyWorkerCanReregister(long workerId, List<RegisterWorkerPRequest> requestChunks, int expectedBlockCount) throws Exception {
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    assertEquals(0, errorQueue.size());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(expectedBlockCount, worker.getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  @Test
  public void workerSendsErrorInStream() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    // An error took place in the worker during the stream
    RegisterWorkerPRequest first = requestChunks.get(0);
    requestObserver.onNext(first);
    Exception e = new IOException("An exception occurred during the stream");
    requestObserver.onError(e);

    // The following requests will be rejected as the stream has been closed
    // on the master side.
    RegisterWorkerPRequest second = requestChunks.get(1);
    requestObserver.onNext(second);
    requestObserver.onCompleted();
    System.out.println("Received errors: " + errorQueue);
    assertEquals(2, errorQueue.size());

    // verify the worker is not registered
    assertEquals(0, mBlockMaster.getWorkerCount());

    verifyWorkerCanReregister(workerId, requestChunks, 100+200+300+1000+1500+2000);
  }

  @Test
  public void workerSendsErrorBeforeCompleting() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }

    // An error took place on the worker side after all the requests have been sent
    Exception e = new IOException("Exception on the worker side");
    requestObserver.onError(e);
    // The complete should be rejected
    requestObserver.onCompleted();
    System.out.println("Received errors: " + errorQueue);
    assertEquals(1, errorQueue.size());

    // verify the worker is not registered
    assertEquals(0, mBlockMaster.getWorkerCount());

    verifyWorkerCanReregister(workerId, requestChunks, 100+200+300+1000+1500+2000);
  }

  /**
   * Tests below cover the race conditions during concurrent executions.
   *
   * When the worker registers for the 1st time, no clients should know this worker.
   * Therefore there is no concurrent client-incurred write operations on this worker.
   * The races happen typically when the worker re-registers with the master,
   * where some clients already know this worker and can direct invoke writes on the worker.
   *
   * Tests here verify the integrity of the master-side metadata.
   * In other words, we assume those writers succeed on the worker, and the subsequent
   * update on the master-side metadata should also succeed and be correct.
   */
  @Test
  public void reregisterWithDelete() throws Exception {
    // Register the worker so the worker is marked active in master
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    long blockToRemove = findFirstBlock(requestChunks);

    // Register again
    System.out.println("Register again");
    CountDownLatch latch = new CountDownLatch(1);
    mExecutorService.submit(() -> {
      sendStreamToMasterAndSignal(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver(), latch);
      System.out.println("register finished");
    });

    // During the register stream, trigger a delete on worker
    System.out.println("waiting for the latch");
    latch.await();
    System.out.println("delete now");
    mBlockMaster.removeBlocks(ImmutableList.of(blockToRemove), true);
    System.out.println("Deleted");

    assertThrows(BlockInfoException.class, () -> {
      mBlockMaster.getBlockInfo(blockToRemove);
    });
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(100+200+300+1000+1500+2000-1, worker.getBlockCount());

    // TODO(jiacheng): usages are incorrect!
    // The block will first be removed from the master metadata when from the worker block list
    // the 1st part will succeed
    // the block will be identified as lost blocks on the worker and removed from the worker
    // block list
    // TODO(jiacheng): no delete command will be issued to the worker
    //  so the copy will still exist on the worker!
    Command command = mBlockMaster.workerHeartbeat(workerId,
            MEM_CAPACITY,
            MEM_USAGE_EMPTY,
            // list of removed blockIds
            ImmutableList.of(),
            ImmutableMap.of(),
            NO_LOST_STORAGE,
            ImmutableList.of());
    System.out.println(command);
  }

  @Test
  public void reregisterWithFree() throws Exception {
    // Register the worker so the worker is marked active in master
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    long blockToRemove = findFirstBlock(requestChunks);

    // Register again
    System.out.println("Register again");
    CountDownLatch latch = new CountDownLatch(1);
    mExecutorService.submit(() -> {
      sendStreamToMasterAndSignal(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver(), latch);
      System.out.println("register finished");
    });

    // During the register stream, trigger a delete on worker
    System.out.println("waiting for the latch");
    latch.await();
    System.out.println("delete now");
    mBlockMaster.removeBlocks(ImmutableList.of(blockToRemove), false);
    System.out.println("Deleted");

    BlockInfo info = mBlockMaster.getBlockInfo(blockToRemove);
    System.out.println(info);
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(1, mBlockMaster.getWorkerCount());
    // The block still exists on the worker but a command will be issued to remove it
    assertEquals(100+200+300+1000+1500+2000, worker.getBlockCount());

    // TODO(jiacheng): usages are incorrect!
    Command command = mBlockMaster.workerHeartbeat(workerId,
            MEM_CAPACITY,
            MEM_USAGE_EMPTY,
            // list of removed blockIds
            ImmutableList.of(),
            ImmutableMap.of(),
            NO_LOST_STORAGE,
            ImmutableList.of());
    System.out.println(command);
    assertEquals(Command.newBuilder().setCommandType(CommandType.Free).addData(blockToRemove).build(), command);
  }

  private long findFirstBlock(List<RegisterWorkerPRequest> chunks) {
    RegisterWorkerPRequest firstBatch = chunks.get(0);
    LocationBlockIdListEntry entry = firstBatch.getCurrentBlocks(0);
    return entry.getValue().getBlockId(0);
  }

  @Test
  public void reregisterWithCommit() throws Exception {
    // Register the worker so the worker is marked active in master
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = generateRegisterStreamForWorker(workerId);
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    long blockToCommit = 100L;
    prepareBLocksOnMaster(ImmutableList.of(blockToCommit));

    // Register again
    System.out.println("Register again");
    CountDownLatch latch = new CountDownLatch(1);
    mExecutorService.submit(() -> {
      sendStreamToMasterAndSignal(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver(), latch);
      System.out.println("register finished");
    });

    // During the register stream, trigger a delete on worker
    System.out.println("waiting for the latch");
    latch.await();
    System.out.println("commit now");
    mBlockMaster.commitBlock(workerId, BLOCK1_LENGTH, "MEM", "MEM", blockToCommit, BLOCK1_LENGTH);
    System.out.println("committed");

    BlockInfo info = mBlockMaster.getBlockInfo(blockToCommit);
    System.out.println(info);
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(1, mBlockMaster.getWorkerCount());
    // The block still exists on the worker but a command will be issued to remove it
    assertEquals(100+200+300+1000+1500+2000+1, worker.getBlockCount());

    // TODO(jiacheng): usages are incorrect!
    Command command = mBlockMaster.workerHeartbeat(workerId,
            MEM_CAPACITY,
            MEM_USAGE_EMPTY,
            // list of removed blockIds
            ImmutableList.of(),
            ImmutableMap.of(),
            NO_LOST_STORAGE,
            ImmutableList.of());
    // Assume the write has succeeded, no command from the master to the worker
    // because everything is now done.
    assertEquals(EMPTY_CMD, command);
  }
}
