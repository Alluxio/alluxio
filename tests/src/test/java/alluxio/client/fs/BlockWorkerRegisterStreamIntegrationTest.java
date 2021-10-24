package alluxio.client.fs;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.client.block.stream.UnderFileSystemFileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.clock.ManualClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BlockMasterWorkerServiceGrpc;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterWorkerServiceHandler;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.block.RegisterStreamer;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.file.FileSystemMasterClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.StatusException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static alluxio.grpc.BlockMasterWorkerServiceGrpc.*;
import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.stress.rpc.TierAlias.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Integration tests for the client-side logic for the register stream.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterWorkerServiceStub.class})
public class BlockWorkerRegisterStreamIntegrationTest {
  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private MetricsMaster mMetricsMaster;

  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
          .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
          .setRpcPort(83).setDataPort(84).setWebPort(85);

  public static final Map<String, List<String>> LOST_STORAGE =
          ImmutableMap.of(MEM.toString(), ImmutableList.of());
  public static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();
  public static final long BLOCK_SIZE = ServerConfiguration.global().getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);

  private BlockMasterClient mBlockMasterClient;
  private BlockMasterClientPool mBlockMasterClientPool;
  private TieredBlockStore mBlockStore;
  private DefaultBlockWorker mBlockWorker;
  private alluxio.worker.file.FileSystemMasterClient mFileSystemMasterClient;
  private Random mRandom;
  private Sessions mSessions;
  private UfsManager mUfsManager;
  private String mMemDir =
          AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_MEM).getAbsolutePath();
  private String mHddDir =
          AlluxioTestDirectory.createTemporaryDirectory(Constants.MEDIUM_HDD).getAbsolutePath();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  @Rule
  public ConfigurationRule mConfigurationRule =
          new ConfigurationRule(new ImmutableMap.Builder<PropertyKey, String>()
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2")
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_ALIAS, Constants.MEDIUM_MEM)
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_MEDIUMTYPE, Constants.MEDIUM_MEM)
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_QUOTA, "1GB")
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL0_DIRS_PATH, mMemDir)
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_ALIAS, Constants.MEDIUM_HDD)
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_MEDIUMTYPE, Constants.MEDIUM_HDD)
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_QUOTA, "2GB")
                  .put(PropertyKey.WORKER_TIERED_STORE_LEVEL1_DIRS_PATH, mHddDir)
                  .put(PropertyKey.WORKER_RPC_PORT, "0")
                  .put(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_RESERVED_BYTES, "0")
                  .build(), ServerConfiguration.global());

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set the config properties
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, true);
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE, 1000);
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT, "1s");
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_COMPLETE_TIMEOUT, "3s");

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

    // Create a BlockWorker to get blocks from
    mRandom = new Random();
    mBlockMasterClient = mock(BlockMasterClient.class);
    mBlockMasterClientPool = spy(new BlockMasterClientPool());
    when(mBlockMasterClientPool.createNewResource()).thenReturn(mBlockMasterClient);
    mBlockStore = spy(new TieredBlockStore());
    mFileSystemMasterClient = mock(FileSystemMasterClient.class);
    mSessions = mock(Sessions.class);
    mUfsManager = mock(UfsManager.class);

    mBlockWorker = new DefaultBlockWorker(mBlockMasterClientPool, mFileSystemMasterClient,
            mSessions, mBlockStore, mUfsManager);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }


  /**
   * Tests below cover the most normal cases.
   */
  @Test
  public void requestsForEmptyWorker() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForEmptyWorker(workerId);

    // Verify the size and content of the requests
    // TODO(jiacheng): verify more fields
    assertEquals(1, requestChunks.size());
    RegisterWorkerPRequest request = requestChunks.get(0);
    assertEquals(workerId, request.getWorkerId());
    List<LocationBlockIdListEntry> entries = request.getCurrentBlocksList();
    assertEquals(0, entries.size());
  }

  @Test
  public void requestsForWorker() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
            RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);

    // Verify the size and content of the requests
    int expectedBatchCount = (int) Math.ceil((100+200+300+1000+1500+2000)/(double)1000);
    assertEquals(expectedBatchCount, requestChunks.size());
    // TODO(jiacheng): manually check more on the requests
    for (int i = 0; i < expectedBatchCount; i++) {
      RegisterWorkerPRequest request = requestChunks.get(i);
      assertEquals(workerId, request.getWorkerId());
      List<LocationBlockIdListEntry> entries = request.getCurrentBlocksList();

      int totalSize = 0;
      for (LocationBlockIdListEntry entry : entries) {
        totalSize += entry.getValue().getBlockIdCount();
      }
      if (i != expectedBatchCount - 1) {
        assertEquals(1000, totalSize);
      }
    }
  }

  // TODO(jiacheng): how to verify the sending logic? verify all batches are received then closed properly?


  /**
   * Tests below cover various failure cases.
   */
  @Test
  public void registerWorkerErrorAtStreamStart() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // TODO(jiacheng): extract these constants
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // We just use the RegisterStreamer to generate the batch of requests
    // TODO(jiacheng): the problem is the client and asyncClient are null!
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.FIRST_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = registerStreamer.mResponseObserver;
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorDuringStream() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // TODO(jiacheng): extract these constants
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // We just use the RegisterStreamer to generate the batch of requests
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.SECOND_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = registerStreamer.mResponseObserver;
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorAtCompletion() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // TODO(jiacheng): extract these constants
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // We just use the RegisterStreamer to generate the batch of requests
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = registerStreamer.mResponseObserver;
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsInStream() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // TODO(jiacheng): extract these constants
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // We just use the RegisterStreamer to generate the batch of requests
    // TODO(jiacheng): find a better way so that the registerStreamer can be run in another class
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.HANG_IN_STREAM);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = registerStreamer.mResponseObserver;
    requestObserver.setResponseObserver(responseObserver);

    // The master will hang during the stream which exceeds the deadline between messages
    assertThrows(DeadlineExceededException.class, () -> {
      // TODO(jiacheng): worker -> master -> worker error forwarding happened, break the chain
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsOnCompleted() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // TODO(jiacheng): extract these constants
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // We just use the RegisterStreamer to generate the batch of requests
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.HANG_ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = registerStreamer.mResponseObserver;
    requestObserver.setResponseObserver(responseObserver);

    // The master will hang during the stream which exceeds the deadline between messages
    assertThrows(DeadlineExceededException.class, () -> {
      // TODO(jiacheng): worker -> master -> worker error forwarding happened, break the chain
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
   * In other words, even a commit/delete happens on the worker during the register stream,
   * the change should be successful and the update should be recorded correctly.
   * The update should later be reported to the master.
   */
  // TODO(jiacheng): register streaming, a delete happened, check the following heartbeat
  // TODO(jiacheng): register streaming, internal block movement happened, check the following heartbeat

  public long getWorkerId(WorkerNetAddress address) throws Exception {
    long workerId = mBlockMaster.getWorkerId(address);
    System.out.println("Worker id " + workerId);
    return workerId;
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

  enum ErrorMode {
    FIRST_REQUEST,
    SECOND_REQUEST,
    ON_COMPLETED,
    HANG_IN_STREAM,
    HANG_ON_COMPLETED
  }

  class TestRequestObserver implements StreamObserver<alluxio.grpc.RegisterWorkerPRequest> {
    private int batch = 0;
    ErrorMode mErrorMode;
    StreamObserver<RegisterWorkerPResponse> mResponseObserver;

    TestRequestObserver(ErrorMode errorMode) {
      mErrorMode = errorMode;
    }

    void setResponseObserver(StreamObserver<RegisterWorkerPResponse> responseObserver) {
      mResponseObserver = responseObserver;
    }

    @Override
    public void onNext(alluxio.grpc.RegisterWorkerPRequest chunk) {
      System.out.println("batch = " + batch + " master received request");
      if (mErrorMode == ErrorMode.HANG_IN_STREAM) {
        System.out.println("No response is equal to infinite waiting");
        return;
      }
      if (batch == 0 && mErrorMode == ErrorMode.FIRST_REQUEST) {
        // Throw a checked exception that is the most likely at this stage
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(new NotFoundException("Simulate worker is not found")));
        batch++;
        return;
      }
      if (batch == 1 && mErrorMode == ErrorMode.SECOND_REQUEST) {
        // There is no checked exception after the 1st request
        // It is probably because something is wrong on the master side
        StatusException x = new InternalException(new RuntimeException("Error on the server side")).toGrpcStatusException();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(x));
        batch++;
        return;
      }
      System.out.println("Master sending response for batch " + batch);
      mResponseObserver.onNext(RegisterWorkerPResponse.getDefaultInstance());
      batch++;
    }

    @Override
    public void onError(Throwable t) {
      System.out.println("Master received error " + t);
      mResponseObserver.onError(t);
    }

    @Override
    public void onCompleted() {
      System.out.println("Master received complete msg ");
      if (mErrorMode == ErrorMode.HANG_ON_COMPLETED) {
        System.out.println("No response is equal to infinite waiting");
        return;
      }
      if (mErrorMode == ErrorMode.ON_COMPLETED) {
        // There is no checked exception after the 1st request
        // It is probably because something is wrong on the master side
        StatusException x = new InternalException(new RuntimeException("Error on completing the server side")).toGrpcStatusException();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(x));
        return;
      }
      mResponseObserver.onCompleted();
    }
  }
}
