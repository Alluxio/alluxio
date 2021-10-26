package alluxio.client.fs;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static alluxio.client.fs.RegisterStreamTestUtils.CAPACITY_MAP;
import static alluxio.client.fs.RegisterStreamTestUtils.MEM_CAPACITY;
import static alluxio.client.fs.RegisterStreamTestUtils.MEM_USAGE_EMPTY;
import static alluxio.client.fs.RegisterStreamTestUtils.USAGE_MAP;
import static alluxio.grpc.BlockMasterWorkerServiceGrpc.*;
import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import static alluxio.client.fs.RegisterStreamTestUtils.parseTierConfig;
import static alluxio.client.fs.RegisterStreamTestUtils.getTierAliases;
import static alluxio.client.fs.RegisterStreamTestUtils.BATCH_SIZE;
import static alluxio.client.fs.RegisterStreamTestUtils.EMPTY_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.LOST_STORAGE;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_BLOCK_TOTAL;

/**
 * Integration tests for the client-side logic for the register stream.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterWorkerServiceStub.class})
public class BlockWorkerRegisterStreamIntegrationTest {
  private static long WORKER_ID = 1L;

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

    // Prepare test data
    mTierAliases = getTierAliases(parseTierConfig(TIER_CONFIG));
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    mBlockMap = RpcBenchPreparationUtils.generateBlockIdOnTiers(parseTierConfig(TIER_CONFIG));
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
    int expectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL)/(double)BATCH_SIZE);
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
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    AtomicInteger receivedCount = new AtomicInteger();
    AtomicBoolean completed = new AtomicBoolean();
    TestRequestObserver requestObserver = new TestRequestObserver(Mode.CORRECT, receivedCount, completed);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
            WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    registerStreamer.registerWithMaster();

    int expectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL)/(double)BATCH_SIZE);
    assertEquals(expectedBatchCount, receivedCount.get());
    assertEquals(true, completed.get());
  }

  // The field is private so we have to use reflection to bypass the permission control
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
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(Mode.FIRST_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorDuringStream() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(Mode.SECOND_REQUEST);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void registerWorkerErrorAtCompletion() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(Mode.ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    assertThrows(InternalException.class, () -> {
      // An error received from the master side will be InternalException
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsInStream() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(Mode.HANG_IN_STREAM);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
    StreamObserver<RegisterWorkerPResponse> responseObserver = getResponseObserver(registerStreamer);
    requestObserver.setResponseObserver(responseObserver);

    // The master will hang during the stream which exceeds the deadline between messages
    assertThrows(DeadlineExceededException.class, () -> {
      registerStreamer.registerWithMaster();
    });
  }

  @Test
  public void masterHangsOnCompleted() throws Exception {
    // Create the stream and control the request/response observers
    BlockMasterWorkerServiceStub asyncClient = PowerMockito.mock(BlockMasterWorkerServiceStub.class);
    when(asyncClient.withDeadlineAfter(anyLong(), any())).thenReturn(asyncClient);
    TestRequestObserver requestObserver = new TestRequestObserver(Mode.HANG_ON_COMPLETED);
    when(asyncClient.registerWorkerStream(any())).thenReturn(requestObserver);
    RegisterStreamer registerStreamer = new RegisterStreamer(asyncClient,
        WORKER_ID, mTierAliases, mCapacityMap, mUsedMap, mBlockMap, LOST_STORAGE, EMPTY_CONFIG);
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

  // Places where the master may throw an error
  enum Mode {
    FIRST_REQUEST,
    SECOND_REQUEST,
    ON_COMPLETED,
    HANG_IN_STREAM,
    HANG_ON_COMPLETED,
    CORRECT
  }

  class TestRequestObserver implements StreamObserver<alluxio.grpc.RegisterWorkerPRequest> {
    private int batch = 0;
    Mode mMode;
    StreamObserver<RegisterWorkerPResponse> mResponseObserver;
    // Used to pass a signal back to the tester
    AtomicInteger mReceivedCount = null;
    AtomicBoolean mCompleted = null;

    TestRequestObserver(Mode mode) {
      mMode = mode;
    }

    TestRequestObserver(Mode mode, AtomicInteger receivedCount, AtomicBoolean completed) {
      this(mode);
      mReceivedCount = receivedCount;
      mCompleted = completed;
    }

    void setResponseObserver(StreamObserver<RegisterWorkerPResponse> responseObserver) {
      mResponseObserver = responseObserver;
    }

    @Override
    public void onNext(alluxio.grpc.RegisterWorkerPRequest chunk) {
      System.out.println("batch = " + batch + " master received request");
      if (mMode == Mode.HANG_IN_STREAM) {
        System.out.println("No response is equal to infinite waiting");
        return;
      }
      if (batch == 0 && mMode == Mode.FIRST_REQUEST) {
        // Throw a checked exception that is the most likely at this stage
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(new NotFoundException("Simulate worker is not found")));
        batch++;
        return;
      }
      if (batch == 1 && mMode == Mode.SECOND_REQUEST) {
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
      if (mMode == Mode.HANG_ON_COMPLETED) {
        System.out.println("No response is equal to infinite waiting");
        return;
      }
      if (mMode == Mode.ON_COMPLETED) {
        // There is no checked exception after the 1st request
        // It is probably because something is wrong on the master side
        StatusException x = new InternalException(new RuntimeException("Error on completing the server side")).toGrpcStatusException();
        mResponseObserver.onError(GrpcExceptionUtils.fromThrowable(x));
        return;
      }
      if (mMode == Mode.CORRECT) {
        mReceivedCount.set(batch);
        mCompleted.set(true);
      }
      mResponseObserver.onCompleted();
    }
  }
}
