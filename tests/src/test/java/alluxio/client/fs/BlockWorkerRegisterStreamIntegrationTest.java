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

package alluxio.client.fs;

import static alluxio.client.fs.RegisterStreamTestUtils.BATCH_SIZE;
import static alluxio.client.fs.RegisterStreamTestUtils.CAPACITY_MAP;
import static alluxio.client.fs.RegisterStreamTestUtils.EMPTY_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.LOST_STORAGE;
import static alluxio.client.fs.RegisterStreamTestUtils.MEM_CAPACITY;
import static alluxio.client.fs.RegisterStreamTestUtils.MEM_USAGE_EMPTY;
import static alluxio.client.fs.RegisterStreamTestUtils.NET_ADDRESS_1;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_BLOCK_TOTAL;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.USAGE_MAP;
import static alluxio.client.fs.RegisterStreamTestUtils.findFirstBlock;
import static alluxio.client.fs.RegisterStreamTestUtils.getTierAliases;
import static alluxio.client.fs.RegisterStreamTestUtils.parseTierConfig;
import static alluxio.grpc.BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub;
import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.Command;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.Metric;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import alluxio.master.MasterClientContext;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.underfs.UfsManager;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockMasterSync;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.block.RegisterStreamer;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for the client-side logic for the register stream.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterWorkerServiceGrpc.BlockMasterWorkerServiceStub.class})
public class BlockWorkerRegisterStreamIntegrationTest {
  private static final long WORKER_ID = 1L;

  // Some fields for made-up data
  private List<String> mTierAliases;
  private Map<String, Long> mCapacityMap;
  private Map<String, Long> mUsedMap;
  private Map<BlockStoreLocation, List<Long>> mBlockMap;
  private Map<String, Integer> mTierToIndex =
      ImmutableMap.of("MEM", 0, "SSD", 1, "HDD", 2);

  // Used to mock the block worker
  private BlockMasterClient mBlockMasterClient;
  private BlockMasterClientPool mBlockMasterClientPool;
  private DefaultBlockWorker mBlockWorker;
  private String mMemDir =
      AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_MEM).getAbsolutePath();
  private String mHddDir =
      AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_HDD).getAbsolutePath();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, Object>()
          .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, 2)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_MEM)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "1GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, mMemDir)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS, Constants.MEDIUM_HDD)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_MEDIUMTYPE, Constants.MEDIUM_HDD)
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA, "2GB")
          .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, mHddDir)
          .put(PropertyKey.WORKER_RPC_PORT, 0)
          .put(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES, "0")
          .put(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
          .build(), Configuration.modifiableGlobal());

  private ExecutorService mExecutorService;

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set the config properties
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, true);
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE, BATCH_SIZE);
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT, "1s");
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_COMPLETE_TIMEOUT, "3s");

    // Prepare test data
    mTierAliases = getTierAliases(parseTierConfig(TIER_CONFIG));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    mBlockMap = RpcBenchPreparationUtils.generateBlockIdOnTiers(parseTierConfig(TIER_CONFIG));

    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockWorker-%d", true));
  }

  @After
  public void after() throws Exception {
    mExecutorService.shutdown();
  }

  public void initBlockWorker() throws Exception {
    // Prepare a block worker
    mBlockMasterClientPool = spy(new BlockMasterClientPool());
    when(mBlockMasterClientPool.createNewResource()).thenReturn(mBlockMasterClient);
    when(mBlockMasterClientPool.acquire()).thenReturn(mBlockMasterClient);
    TieredBlockStore blockStore = spy(new TieredBlockStore());
    FileSystemMasterClient fileSystemMasterClient = mock(FileSystemMasterClient.class);
    Sessions sessions = mock(Sessions.class);
    UfsManager ufsManager = mock(UfsManager.class);

    mBlockWorker = new DefaultBlockWorker(mBlockMasterClientPool, fileSystemMasterClient,
            sessions, blockStore, ufsManager);
  }

  /**
   * Tests below cover the most normal cases.
   */
  @Test
  public void requestsForEmptyWorker() throws Exception {
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForEmptyWorker(WORKER_ID);

    // Verify the size and content of the requests
    assertEquals(1, requestChunks.size());
    RegisterWorkerPRequest request = requestChunks.get(0);
    assertEquals(WORKER_ID, request.getWorkerId());
    assertEquals(MEM_USAGE_EMPTY, request.getUsedBytesOnTiersMap());
    assertEquals(MEM_CAPACITY, request.getTotalBytesOnTiersMap());
    Map<String, StorageList> lostMap = request.getLostStorageMap();
    assertEquals(1, lostMap.size());
    assertEquals(StorageList.newBuilder().build(), lostMap.get("MEM"));
    assertEquals(ImmutableList.of("MEM"), request.getStorageTiersList());

    List<LocationBlockIdListEntry> entries = request.getCurrentBlocksList();
    assertEquals(0, entries.size());
  }

  @Test
  public void requestsForWorker() throws Exception {
    List<RegisterWorkerPRequest> requestChunks =
            RegisterStreamTestUtils.generateRegisterStreamForWorker(WORKER_ID);

    // Verify the size and content of the requests
    int expectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL) / (double) BATCH_SIZE);
    Set<Long> containedBlockIds = new HashSet<>();
    assertEquals(expectedBatchCount, requestChunks.size());
    for (int i = 0; i < expectedBatchCount; i++) {
      RegisterWorkerPRequest request = requestChunks.get(i);
      assertEquals(WORKER_ID, request.getWorkerId());
      List<LocationBlockIdListEntry> entries = request.getCurrentBlocksList();

      int totalSize = 0;
      for (LocationBlockIdListEntry entry : entries) {
        totalSize += entry.getValue().getBlockIdCount();
        containedBlockIds.addAll(entry.getValue().getBlockIdList());
      }
      if (i != expectedBatchCount - 1) {
        assertEquals(BATCH_SIZE, totalSize);
      }

      // The 1st request contains metadata but the following do not
      if (i == 0) {
        assertEquals(USAGE_MAP, request.getUsedBytesOnTiersMap());
        assertEquals(CAPACITY_MAP, request.getTotalBytesOnTiersMap());
        Map<String, StorageList> lostMap = request.getLostStorageMap();
        assertEquals(1, lostMap.size());
        assertEquals(StorageList.newBuilder().build(), lostMap.get("MEM"));
        assertEquals(ImmutableList.of("MEM", "SSD", "HDD"), request.getStorageTiersList());
      } else {
        assertEquals(0, request.getStorageTiersCount());
        assertEquals(ImmutableMap.of(), request.getUsedBytesOnTiersMap());
        assertEquals(ImmutableMap.of(), request.getTotalBytesOnTiersMap());
        Map<String, StorageList> lostMap = request.getLostStorageMap();
        assertEquals(0, lostMap.size());
      }
    }
    assertEquals(containedBlockIds.size(), TIER_BLOCK_TOTAL);
  }

  @Test
  public void normalFlow() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient =
        PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    AtomicInteger receivedCount = new AtomicInteger();
    AtomicBoolean completed = new AtomicBoolean();
    ManualRegisterStreamObserver requestObserver =
        new ManualRegisterStreamObserver(
            MasterMode.COMPLETE_AND_REPORT, receivedCount, completed);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver =
        getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    registerStreamer.registerWithMaster();

    int expectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL) / (double) BATCH_SIZE);
    assertEquals(expectedBatchCount, receivedCount.get());
    assertEquals(true, completed.get());
  }

  // The field is private so we have to use reflection to bypass the permission control
  private StreamObserver<RegisterWorkerPResponse> getResponseObserver(
      RegisterStreamer stream) throws Exception {
    Field privateField
        = RegisterStreamer.class.getDeclaredField("mMasterResponseObserver");
    privateField.setAccessible(true);
    return (StreamObserver<RegisterWorkerPResponse>) privateField.get(stream);
  }

  /**
   * Tests below cover various failure cases.
   */
  @Test
  public void registerWorkerErrorAtStreamStart() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient =
        PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    ManualRegisterStreamObserver requestObserver =
        new ManualRegisterStreamObserver(MasterMode.FIRST_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver =
        getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorDuringStream() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient =
        PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    ManualRegisterStreamObserver requestObserver =
        new ManualRegisterStreamObserver(MasterMode.SECOND_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver =
        getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorAtCompletion() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient =
        PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    ManualRegisterStreamObserver requestObserver =
        new ManualRegisterStreamObserver(MasterMode.ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver =
        getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsInStream() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient =
        PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    ManualRegisterStreamObserver requestObserver =
        new ManualRegisterStreamObserver(MasterMode.HANG_IN_STREAM);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver =
        getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    // The master will hang during the stream which exceeds the deadline between messages
    assertThrows(DeadlineExceededException.class, () -> {
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsOnCompleted() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient =
        PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    ManualRegisterStreamObserver requestObserver =
        new ManualRegisterStreamObserver(MasterMode.HANG_ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver =
        getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    // The master will hang at the completion step which exceeds the deadline between messages
    assertThrows(DeadlineExceededException.class, () -> {
      registerStreamer.registerWithMaster();
    });
  }

  /**
   * Tests below cover the race conditions during concurrent executions.
   *
   * If a worker is new to the cluster, no clients should know this worker.
   * Therefore there is no concurrent client-incurred write operations on this worker.
   * The races happen typically when the worker re-registers with the master,
   * where some clients already know this worker and can direct invoke writes on the worker.
   *
   * Tests here verify the integrity of the worker-side metadata.
   * In other words, even a delete/move happens on the worker during the register stream,
   * the change should be successful and the update should be recorded correctly.
   * The update should later be reported to the master.
   */
  @Test
  public void deleteDuringRegisterStream() throws Exception {
    // Generate a request stream of blocks
    List<RegisterWorkerPRequest> requestChunks =
          RegisterStreamTestUtils.generateRegisterStreamForWorker(WORKER_ID);
    // Select a block to remove concurrent with the stream
    long blockToRemove = findFirstBlock(requestChunks);

    // Manually create a stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient =
        PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    CountDownLatch signalLatch = new CountDownLatch(1);
    ManualRegisterStreamObserver requestObserver =
        new ManualRegisterStreamObserver(MasterMode.SIGNAL_IN_STREAM, signalLatch);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver =
        getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    // Prepare the block worker to use the overriden stream
    MasterClientContext context = MasterClientContext
            .newBuilder(ClientContext.create(Configuration.global())).build();
    // On heartbeat, the expected values will be checked against
    List<Long> expectedLostBlocks = ImmutableList.of(blockToRemove);
    Map<BlockStoreLocation, List<Long>> expectedAddedBlocks = ImmutableMap.of();
    mBlockMasterClient = new TestBlockMasterClient(context, registerStreamer,
        expectedLostBlocks, expectedAddedBlocks);
    initBlockWorker();
    // Prepare the blocks in the BlockWorker storage that match the register stream
    prepareBlocksOnWorker(TIER_CONFIG);

    // Let a delete job wait on the signal
    AtomicReference<Throwable> error = new AtomicReference<>();
    Future f = mExecutorService.submit(() -> {
      try {
        signalLatch.await();
        mBlockWorker.removeBlock(1L, blockToRemove);
      } catch (Exception e) {
        error.set(e);
      }
    });

    // Kick off the register stream
    AtomicReference<Long> workerId = new AtomicReference<>(WORKER_ID);
    AtomicReference<String> clusterId = new AtomicReference<>(UUID.randomUUID().toString());
    BlockMasterSync sync = new BlockMasterSync(
        mBlockWorker, workerId, clusterId, NET_ADDRESS_1, mBlockMasterClientPool);

    // Check the next heartbeat to be sent to the master
    f.get();
    assertNull(error.get());
    // Validation will happen on the heartbeat
    sync.heartbeat();
  }

  // TODO(jiacheng): an internal block movement happens during register stream
  //  verify the next heartbeat

  // An overriden BlockMasterClient
  class TestBlockMasterClient extends BlockMasterClient {
    RegisterStreamer mStreamer;
    List<Long> mRemovedBlocks;
    Map<BlockStoreLocation, List<Long>> mAddedBlocks;

    public TestBlockMasterClient(MasterClientContext conf, RegisterStreamer streamer,
         final List<Long> removedBlocks, final Map<BlockStoreLocation, List<Long>> addedBlocks) {
      super(conf);
      mStreamer = streamer;
      mRemovedBlocks = removedBlocks;
      mAddedBlocks = addedBlocks;
    }

    // Just use the prepared stream
    @Override
    public void registerWithStream(
        final long workerId, final List<String> storageTierAliases,
        final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
        final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
        final Map<String, List<String>> lostStorage,
        final List<ConfigProperty> configList) throws IOException {
      AtomicReference<IOException> ioe = new AtomicReference<>();
      try {
        mStreamer.registerWithMaster();
      } catch (IOException e) {
        ioe.set(e);
      } catch (InterruptedException e) {
        ioe.set(new IOException(e));
      }
      if (ioe.get() != null) {
        throw ioe.get();
      }
      return;
    }

    @Override
    public synchronized Command heartbeat(
        final long workerId, String clusterId, final Map<String, Long> capacityBytesOnTiers,
        final Map<String, Long> usedBytesOnTiers,
        final List<Long> removedBlocks, final Map<BlockStoreLocation, List<Long>> addedBlocks,
        final Map<String, List<String>> lostStorage, final List<Metric> metrics) {
      assertEquals(mRemovedBlocks, removedBlocks);
      assertEquals(mAddedBlocks, addedBlocks);
      return Command.getDefaultInstance();
    }

    @Override
    public void commitBlock(final long workerId, final long usedBytesOnTier,
        final String tierAlias, final String mediumType,
        final long blockId, final long length) throws IOException {
      // Noop because there is no master
    }
  }

  private void prepareBlocksOnWorker(String tierConfig) throws Exception {
    List<String> tierAliases = getTierAliases(parseTierConfig(tierConfig));
    // Generate block IDs heuristically
    Map<TierAlias, List<Integer>> tierConfigMap = parseTierConfig(tierConfig);
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(tierConfigMap);

    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockMap.entrySet()) {
      BlockStoreLocation loc = entry.getKey();
      int tierIndex = mTierToIndex.get(loc.tierAlias());
      for (long blockId : entry.getValue()) {
        mBlockWorker.createBlock(1L, blockId, tierIndex,
            new CreateBlockOptions(null, loc.tierAlias(), 1));
        mBlockWorker.commitBlock(1L, blockId, false);
      }
    }
  }

  // Controls the behavior of the manually overriden master-side register stream observer
  enum MasterMode {
    FIRST_REQUEST,
    SECOND_REQUEST,
    ON_COMPLETED,
    HANG_IN_STREAM,
    HANG_ON_COMPLETED,
    COMPLETE_AND_REPORT,
    SIGNAL_IN_STREAM
  }

  // A master side register stream handler that may return error on certain calls
  class ManualRegisterStreamObserver implements StreamObserver<RegisterWorkerPRequest> {
    private int mBatch = 0;
    MasterMode mMasterMode;
    StreamObserver<RegisterWorkerPResponse> mResponseObserver;
    // Used to pass a signal back to the tester
    AtomicInteger mReceivedCount = null;
    AtomicBoolean mCompleted = null;
    CountDownLatch mSignalLatch = null;

    ManualRegisterStreamObserver(MasterMode masterMode) {
      mMasterMode = masterMode;
    }

    ManualRegisterStreamObserver(MasterMode masterMode,
        AtomicInteger receivedCount, AtomicBoolean completed) {
      this(masterMode);
      mReceivedCount = receivedCount;
      mCompleted = completed;
    }

    ManualRegisterStreamObserver(MasterMode masterMode, CountDownLatch signalLatch) {
      this(masterMode);
      mSignalLatch = signalLatch;
    }

    void setResponseObserver(StreamObserver<RegisterWorkerPResponse> responseObserver) {
      mResponseObserver = responseObserver;
    }

    @Override
    public void onNext(alluxio.grpc.RegisterWorkerPRequest chunk) {
      if (mMasterMode == MasterMode.HANG_IN_STREAM) {
        return;
      }
      if (mBatch == 0 && mMasterMode == MasterMode.FIRST_REQUEST) {
        // Throw a checked exception that is the most likely at this stage
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(
            new NotFoundException("Simulate worker is not found")));
        mBatch++;
        return;
      }
      if (mBatch == 1 && mMasterMode == MasterMode.SECOND_REQUEST) {
        // There is no checked exception after the 1st request
        // It is probably because something is wrong on the master side
        StatusException x = new InternalException(
            new RuntimeException("Error on the server side")).toGrpcStatusException();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(x));
        mBatch++;
        return;
      }
      mResponseObserver.onNext(RegisterWorkerPResponse.getDefaultInstance());
      if (mBatch == 0 && mMasterMode == MasterMode.SIGNAL_IN_STREAM) {
        // Send a signal and continue
        mSignalLatch.countDown();
      }
      mBatch++;
    }

    @Override
    public void onError(Throwable t) {
      mResponseObserver.onError(t);
    }

    @Override
    public void onCompleted() {
      if (mMasterMode == MasterMode.HANG_ON_COMPLETED) {
        // No response is equal to infinite waiting
        return;
      }
      if (mMasterMode == MasterMode.ON_COMPLETED) {
        // There is no checked exception after the 1st request
        // It is probably because something is wrong on the master side
        StatusException x = new InternalException(
            new RuntimeException("Error on completing the server side")).toGrpcStatusException();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(x));
        return;
      }
      if (mMasterMode == MasterMode.COMPLETE_AND_REPORT) {
        mReceivedCount.set(mBatch);
        mCompleted.set(true);
      }
      mResponseObserver.onCompleted();
    }
  }
}
