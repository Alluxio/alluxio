package alluxio.client.fs;

import alluxio.clock.ManualClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
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
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static alluxio.grpc.BlockMasterWorkerServiceGrpc.*;
import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.stress.rpc.TierAlias.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import static alluxio.client.fs.RegisterStreamTestUtils.convert;
import static alluxio.client.fs.RegisterStreamTestUtils.getTierAliases;
import static alluxio.client.fs.RegisterStreamTestUtils.BATCH_SIZE;
import static alluxio.client.fs.RegisterStreamTestUtils.EMPTY_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.LOST_STORAGE;
import static alluxio.client.fs.RegisterStreamTestUtils.NET_ADDRESS_1;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_BLOCK_TOTAL;


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

  List<String> mTierAliases;
  Map<String, Long> mCapacityMap;
  Map<String, Long> mUsedMap;
  Map<BlockStoreLocation, List<Long>> mBlockMap;

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set the config properties
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, true);
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE, BATCH_SIZE);
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT, "1s");
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_COMPLETE_TIMEOUT, "3s");

    // TODO(jiacheng): no need for block master?
    mRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    mClock = new ManualClock();

    mExecutorService =
            Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mBlockMaster = new DefaultBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);

    // Prepare test data
    mTierAliases = getTierAliases(convert(TIER_CONFIG));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    mBlockMap = RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(TIER_CONFIG));
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
    int expectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL)/(double)BATCH_SIZE);
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
        assertEquals(BATCH_SIZE, totalSize);
      }
    }
  }

  // TODO(jiacheng): how to verify the sending logic? verify all batches are received then closed properly?


  private StreamObserver<RegisterWorkerPResponse> getResponseObserver(RegisterStreamer stream) throws Exception {
    Field privateField
            = RegisterStreamer.class.getDeclaredField("mResponseObserver");
    privateField.setAccessible(true);
    return (StreamObserver<RegisterWorkerPResponse>) privateField.get(stream);
  }

  /**
   * Tests below cover various failure cases.
   */
  @Test
  public void registerWorkerErrorAtStreamStart() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.FIRST_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        workerId, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorDuringStream() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.SECOND_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorAtCompletion() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsInStream() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.HANG_IN_STREAM);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    // The master will hang during the stream which exceeds the deadline between messages
    assertThrows(DeadlineExceededException.class, () -> {
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsOnCompleted() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);

    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(ErrorMode.HANG_ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            workerId, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
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

  // Places where the master may throw an error
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
