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
import static alluxio.client.fs.RegisterStreamTestUtils.MEM_CAPACITY_BYTES;
import static alluxio.client.fs.RegisterStreamTestUtils.NET_ADDRESS_1;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_BLOCK_TOTAL;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.USAGE_MAP;
import static alluxio.client.fs.RegisterStreamTestUtils.getErrorCapturingResponseObserver;
import static alluxio.client.fs.RegisterStreamTestUtils.parseTierConfig;
import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

import alluxio.clock.ManualClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.exception.BlockInfoException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterTestUtils;
import alluxio.master.block.BlockMasterWorkerServiceHandler;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.block.RegisterStreamObserver;
import alluxio.master.block.WorkerRegisterContext;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;
import javolution.testing.AssertionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Integration tests for the server-side logic for the register stream.
 */
public class BlockMasterRegisterStreamIntegrationTest {
  private BlockMaster mBlockMaster;
  private CoreMasterContext mMasterContext;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private MetricsMaster mMetricsMaster;
  private BlockMasterWorkerServiceHandler mHandler;

  private static final int MASTER_WORKER_TIMEOUT = 1_000_000;
  private static final long BLOCK_SIZE =
      Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final Command EMPTY_CMD =
      Command.newBuilder().setCommandType(CommandType.Nothing).build();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    // Set the config properties
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_ENABLED, true);
    Configuration.set(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE, BATCH_SIZE);
    Configuration.set(PropertyKey.MASTER_WORKER_TIMEOUT_MS, MASTER_WORKER_TIMEOUT);
    Configuration.set(PropertyKey.MASTER_WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT, "1s");
    Configuration.set(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false);

    mRegistry = new MasterRegistry();
    mMasterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, mMasterContext);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    mClock = new ManualClock();

    mExecutorService =
        Executors.newFixedThreadPool(10, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mBlockMaster = spy(new DefaultBlockMaster(mMetricsMaster, mMasterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService)));
    // ClusterId needs to be generated by metaMaster, so mock it
    doReturn(CLUSTER_ID).when(mBlockMaster).getClusterId();
    mRegistry.add(BlockMaster.class, mBlockMaster);
    mRegistry.start(true);
    mHandler = new BlockMasterWorkerServiceHandler(mBlockMaster);
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  /**
   * Tests below cover the most normal cases.
   */
  @Test
  public void registerEmptyWorkerStream() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForEmptyWorker(workerId);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    // Verify the worker is registered
    assertEquals(0, errorQueue.size());
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(0, worker.getBlockCount());
    assertEquals(0, worker.getToRemoveBlockCount());
    assertEquals(MEM_CAPACITY_BYTES, worker.getCapacityBytes());
    assertEquals(MEM_CAPACITY_BYTES, worker.getAvailableBytes());
    assertEquals(0L, worker.getUsedBytes());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  public void registerWorkerStream() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    // Verify the worker is registered
    assertEquals(0, errorQueue.size());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL, worker.getBlockCount());
    assertEquals(0, worker.getToRemoveBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  // The worker once registered with the master and has lost heartbeat.
  // The master has marked the worker as lost.
  public void registerLostWorker() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    // The worker registers to the master
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    // Verify the worker has been registered
    assertEquals(0, errorQueue.size());
    assertEquals(1, mBlockMaster.getWorkerCount());

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
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));

    // Verify the worker is registered again
    assertEquals(0, errorQueue.size());
    MasterWorkerInfo updatedWorker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL, updatedWorker.getBlockCount());
    assertEquals(0, updatedWorker.getToRemoveBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  // The worker has not yet been forgotten by the master but attempts to register again.
  // This can happen when a worker process is restarted.
  public void registerExistingWorker() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    // Verify the worker has registered
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Register again
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));
    assertEquals(0, newErrorQueue.size());

    // Verify the worker is registered
    MasterWorkerInfo updatedWorker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL, updatedWorker.getBlockCount());
    assertEquals(0, updatedWorker.getToRemoveBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  public void registerExistingWorkerBlocksLost() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    // Register the worker for the 1st time
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    // Verify the worker has registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL, worker.getBlockCount());
    assertEquals(0, worker.getToRemoveBlockCount());

    // Manually generate the blocks again and remove some
    List<String> tierAliases = getTierAliases(parseTierConfig(TIER_CONFIG));
    Map<BlockStoreLocation, List<Long>> blockMap =
        RpcBenchPreparationUtils.generateBlockIdOnTiers(parseTierConfig(TIER_CONFIG));
    Set<Long> lostBlocks = removeSomeBlocks(blockMap);
    // Regenerate the requests
    RegisterStreamer newRegisterStreamer = new RegisterStreamer(null,
        workerId, tierAliases, CAPACITY_MAP, USAGE_MAP, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    List<RegisterWorkerPRequest> newRequestChunks = ImmutableList.copyOf(newRegisterStreamer);
    int newExpectedBatchCount =
        (int) Math.ceil((TIER_BLOCK_TOTAL - lostBlocks.size()) / (double) BATCH_SIZE);
    assertEquals(newExpectedBatchCount, newRequestChunks.size());

    // Register again with the updated stream
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(newRequestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));
    assertEquals(0, newErrorQueue.size());

    // Verify the worker is registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo updatedWorker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL - lostBlocks.size(), updatedWorker.getBlockCount());
    // The master will mark the lost blocks as to be removed
    // This is to ensure the unrecognized blocks do no live on the worker anymore
    assertEquals(lostBlocks.size(), updatedWorker.getToRemoveBlockCount());

    // The update is received during the registration so no command to send to the worker
    Command command = sendHeartbeatToMaster(workerId);
    assertEquals(CommandType.Free, command.getCommandType());
    assertEquals(lostBlocks, new HashSet<>(command.getDataList()));

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  public void registerExistingWorkerBlocksAdded() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    // Register the worker for the 1st time
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    // Verify the worker has registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL, worker.getBlockCount());
    assertEquals(0, worker.getToRemoveBlockCount());

    // Generate block IDs in the same way but add some more
    Map<BlockStoreLocation, List<Long>> blockMap =
        RpcBenchPreparationUtils.generateBlockIdOnTiers(parseTierConfig(TIER_CONFIG));
    Set<Long> addedBlocks = addSomeBlocks(blockMap);
    // Make the master accept these blocks
    prepareBlocksOnMaster(addedBlocks);

    // Re-generate the request
    List<String> tierAliases = getTierAliases(parseTierConfig(TIER_CONFIG));
    Map<String, Long> capacityMap = Maps.toMap(tierAliases, (tier) -> CAPACITY);
    Map<String, Long> usedMap = Maps.toMap(tierAliases, (tier) -> 0L);
    RegisterStreamer newRegisterStreamer = new RegisterStreamer(null,
        workerId, tierAliases, capacityMap, usedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    List<RegisterWorkerPRequest> newRequestChunks = ImmutableList.copyOf(newRegisterStreamer);
    int newExpectedBatchCount =
        (int) Math.ceil((TIER_BLOCK_TOTAL + addedBlocks.size()) / (double) BATCH_SIZE);
    assertEquals(newExpectedBatchCount, newRequestChunks.size());

    // Register again with the new request stream
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(newRequestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));
    assertEquals(0, newErrorQueue.size());

    // Verify the worker is registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo updatedWorker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL + addedBlocks.size(), updatedWorker.getBlockCount());
    assertEquals(0, updatedWorker.getToRemoveBlockCount());

    // No command from the master because the update is received during registration
    assertEquals(EMPTY_CMD, sendHeartbeatToMaster(workerId));

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  /**
   * Tests below cover various failure cases.
   */
  @Test
  public void hangingWorkerSessionRecycled() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
        mHandler.registerWorkerStream(getErrorCapturingResponseObserver(errorQueue));

    // Feed the chunks into the requestObserver
    for (int i = 0; i < requestChunks.size(); i++) {
      RegisterWorkerPRequest chunk = requestChunks.get(i);
      // From the 2nd request on, the request will be rejected
      requestObserver.onNext(chunk);
      if (i == 0) {
        // Progress the clock so the worker stream expired
        mClock.addTime(Duration.of(100_000_000, ChronoUnit.MILLIS));
        // Sleep and wait for the stream recycler thread heartbeat
        CommonUtils.sleepMs(3000);
      }
    }
    // This will be rejected too
    requestObserver.onCompleted();
    // The 5 requests after expiry will be rejected
    // And the complete message will be rejected too
    // -1 because the 1st request was accepted
    // +1 because master sends the TimeoutException to worker on timeout
    // +count because all following requests are rejected
    // +1 because the onComplete() is also rejected
    assertEquals(requestChunks.size() - 1 + 1 + 1, errorQueue.size());

    // Verify the session is recycled
    assertEquals(0, mBlockMaster.getWorkerCount());

    // Verify the worker can re-register and be operated, so the locks are managed correctly
    verifyWorkerCanReregister(workerId, requestChunks, TIER_BLOCK_TOTAL);
    verifyWorkerWritable(workerId);
  }

  @Test
  public void workerSendsErrorOnStart() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
        mHandler.registerWorkerStream(
            RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    // Instead of sending requests to the master, the worker is interrupted
    // around the beginning of the stream. The error propagated to the master.
    Exception e = new InterruptedException("Worker is interrupted");
    assertThrows(IllegalStateException.class, () -> {
      requestObserver.onError(e);
    });

    // After the requestObserver.onError(), no requests
    // shall be accepted by the master.
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    assertEquals(requestChunks.size() + 1, errorQueue.size());

    // verify the worker is not registered
    assertEquals(0, mBlockMaster.getWorkerCount());

    verifyWorkerCanReregister(workerId, requestChunks, TIER_BLOCK_TOTAL);
  }

  @Test
  public void workerSendsErrorInStream() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
        mHandler.registerWorkerStream(
            RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

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
    assertEquals(2, errorQueue.size());

    // verify the worker is not registered
    assertEquals(0, mBlockMaster.getWorkerCount());

    verifyWorkerCanReregister(workerId, requestChunks, TIER_BLOCK_TOTAL);
  }

  @Test
  public void workerSendsErrorBeforeCompleting() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

    // Send the requests to the master
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
        mHandler.registerWorkerStream(
            RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }

    // An error took place on the worker side after all the requests have been sent
    Exception e = new IOException("Exception on the worker side");
    requestObserver.onError(e);
    // The complete should be rejected
    requestObserver.onCompleted();
    assertEquals(1, errorQueue.size());
    // verify the worker is not registered
    assertEquals(0, mBlockMaster.getWorkerCount());

    verifyWorkerCanReregister(workerId, requestChunks, TIER_BLOCK_TOTAL);
  }

  @Test
  public void workerRegisterStartThrowsError() throws Exception {
    // Hijack the block master so it throws errors
    BrokenBlockMaster brokenBlockMaster = new BrokenBlockMaster(
        mMetricsMaster, mMasterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService),
        WorkerRegisterMode.ERROR_START);
    mBlockMaster = brokenBlockMaster;
    mHandler = new BlockMasterWorkerServiceHandler(brokenBlockMaster);

    long workerId = brokenBlockMaster.getWorkerId(NET_ADDRESS_1);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    RegisterStreamObserver streamOb =
        new RegisterStreamObserver(brokenBlockMaster,
            getErrorCapturingResponseObserver(errorQueue));

    // Generate requests
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    for (RegisterWorkerPRequest chunk : requestChunks) {
      streamOb.onNext(chunk);
    }
    streamOb.onCompleted();
    // All requests + onCompleted should receive an error
    assertEquals(requestChunks.size() + 1, errorQueue.size());

    // The BlockMaster is not throwing error this time
    brokenBlockMaster.setCorrect();
    verifyWorkerCanReregister(workerId, requestChunks, TIER_BLOCK_TOTAL);
  }

  @Test
  public void workerRegisterBatchThrowsError() throws Exception {
    // Hijack the block master so it throws errors
    BrokenBlockMaster brokenBlockMaster = new BrokenBlockMaster(
        mMetricsMaster, mMasterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService),
        WorkerRegisterMode.ERROR_STREAM);
    mBlockMaster = brokenBlockMaster;
    mHandler = new BlockMasterWorkerServiceHandler(brokenBlockMaster);

    long workerId = brokenBlockMaster.getWorkerId(NET_ADDRESS_1);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    RegisterStreamObserver streamOb =
        new RegisterStreamObserver(brokenBlockMaster,
            getErrorCapturingResponseObserver(errorQueue));

    // Generate requests
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    for (RegisterWorkerPRequest chunk : requestChunks) {
      streamOb.onNext(chunk);
    }
    streamOb.onCompleted();
    // All requests except the 1st one + onCompleted should receive an error
    assertEquals(requestChunks.size() - 1 + 1, errorQueue.size());

    // The BlockMaster is not throwing error this time
    brokenBlockMaster.setCorrect();
    verifyWorkerCanReregister(workerId, requestChunks, TIER_BLOCK_TOTAL);
  }

  @Test
  public void workerRegisterCompleteThrowsError() throws Exception {
    // Hijack the block master so it throws errors
    BrokenBlockMaster brokenBlockMaster = new BrokenBlockMaster(
        mMetricsMaster, mMasterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService),
        WorkerRegisterMode.ERROR_COMPLETE);
    mBlockMaster = brokenBlockMaster;
    mHandler = new BlockMasterWorkerServiceHandler(brokenBlockMaster);

    long workerId = brokenBlockMaster.getWorkerId(NET_ADDRESS_1);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    RegisterStreamObserver streamOb =
        new RegisterStreamObserver(brokenBlockMaster,
            getErrorCapturingResponseObserver(errorQueue));

    // Generate requests
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    for (RegisterWorkerPRequest chunk : requestChunks) {
      streamOb.onNext(chunk);
    }
    streamOb.onCompleted();
    // Only onCompleted should receive an error
    assertEquals(1, errorQueue.size());

    // The BlockMaster is not throwing error this time
    brokenBlockMaster.setCorrect();
    verifyWorkerCanReregister(workerId, requestChunks, TIER_BLOCK_TOTAL);
  }

  // Different modes that defines how the manually overriden block master work
  enum WorkerRegisterMode {
    ERROR_START,
    ERROR_STREAM,
    ERROR_COMPLETE,
    OK
  }

  // A BlockMaster that can throws error on certain calls
  class BrokenBlockMaster extends DefaultBlockMaster {
    private WorkerRegisterMode mMode;

    public BrokenBlockMaster(MetricsMaster metricsMaster, CoreMasterContext masterContext,
         Clock clock, ExecutorServiceFactory executorServiceFactory, WorkerRegisterMode mode) {
      super(metricsMaster, masterContext, clock, executorServiceFactory);
      mMode = mode;
    }

    public void setCorrect() {
      mMode = WorkerRegisterMode.OK;
    }

    @Override
    public void workerRegisterStart(WorkerRegisterContext context, RegisterWorkerPRequest chunk) {
      if (mMode == WorkerRegisterMode.ERROR_START) {
        RuntimeException e = new AssertionException("Error from workerRegisterStart");
        throw e;
      }
      super.workerRegisterStart(context, chunk);
    }

    @Override
    public void workerRegisterBatch(WorkerRegisterContext context, RegisterWorkerPRequest chunk) {
      if (mMode == WorkerRegisterMode.ERROR_STREAM) {
        RuntimeException e = new AssertionException("Error from workerRegisterBatch");
        throw e;
      }
      super.workerRegisterBatch(context, chunk);
    }

    @Override
    public void workerRegisterFinish(WorkerRegisterContext context) {
      if (mMode == WorkerRegisterMode.ERROR_COMPLETE) {
        RuntimeException e = new AssertionException("Error from workerRegisterBatch");
        throw e;
      }
      super.workerRegisterFinish(context);
    }
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
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Find a block to remove
    long blockToRemove = RegisterStreamTestUtils.findFirstBlock(requestChunks);

    // Register again
    CountDownLatch latch = new CountDownLatch(1);
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    Future f = mExecutorService.submit(() -> {
      sendStreamToMasterAndSignal(requestChunks,
          RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue), latch);
    });

    // During the register stream, trigger a delete on worker
    latch.await();
    mBlockMaster.removeBlocks(ImmutableList.of(blockToRemove), true);

    // Wait for the register to finish
    f.get();

    assertThrows(BlockInfoException.class, () -> {
      mBlockMaster.getBlockInfo(blockToRemove);
    });
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(TIER_BLOCK_TOTAL - 1, worker.getBlockCount());

    // BlockMaster.removeBlocks() will first remove the block from master metadata
    // (with block lock) then update the block locations (with worker lock).
    // The worker lock is being held by the registering worker, but the 1st part
    // will likely succeed.
    // So during registration when checking on the block, the block is not recognized
    // any more and will remain in MasterWorkerInfo.mToRemoveBlocks.
    // In the next heartbeat the master will issue a command to remove the block
    // from the worker.
    // Even if the block is already removed on the worker it is fine,
    // because deletion of a not-found block is a noop.
    Command command = sendHeartbeatToMaster(workerId);
    assertEquals(Command.newBuilder()
        .addData(blockToRemove).setCommandType(CommandType.Free).build(), command);
  }

  @Test
  public void reregisterWithFree() throws Exception {
    // Register the worker so the worker is marked active in master
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks =
        RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Find a block to free
    long blockToRemove = RegisterStreamTestUtils.findFirstBlock(requestChunks);

    // Register again
    CountDownLatch latch = new CountDownLatch(1);
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    mExecutorService.submit(() -> {
      sendStreamToMasterAndSignal(requestChunks,
          RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue), latch);
    });

    // During the register stream, trigger a delete on worker
    latch.await();
    mBlockMaster.removeBlocks(ImmutableList.of(blockToRemove), false);

    BlockInfo info = mBlockMaster.getBlockInfo(blockToRemove);
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(0, newErrorQueue.size());
    assertEquals(1, mBlockMaster.getWorkerCount());
    // The block still exists on the worker but a command will be issued to remove it
    assertEquals(TIER_BLOCK_TOTAL, worker.getBlockCount());

    Command command = sendHeartbeatToMaster(workerId);
    assertEquals(Command.newBuilder()
        .setCommandType(CommandType.Free).addData(blockToRemove).build(), command);
  }

  private void prepareBlocksOnMaster(Collection<Long> blockIds) throws UnavailableException {
    for (long id : blockIds) {
      mBlockMaster.commitBlockInUFS(id, BLOCK_SIZE);
    }
  }

  private void prepareBlocksOnMaster(List<RegisterWorkerPRequest> requestChunks) throws Exception {
    for (RegisterWorkerPRequest chunk : requestChunks) {
      List<LocationBlockIdListEntry> entries = chunk.getCurrentBlocksList();
      for (LocationBlockIdListEntry entry : entries) {
        for (long blockId : entry.getValue().getBlockIdList()) {
          mBlockMaster.commitBlockInUFS(blockId, BLOCK_SIZE);
        }
      }
    }
  }

  private void sendStreamToMaster(List<RegisterWorkerPRequest> requestChunks,
      StreamObserver<RegisterWorkerPResponse> responseObserver) {
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(responseObserver);
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
  }

  private void sendStreamToMasterAndSignal(
      List<RegisterWorkerPRequest> requestChunks,
      StreamObserver<RegisterWorkerPResponse> responseObserver,
      CountDownLatch latch) {
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(responseObserver);
    for (int i = 0; i < requestChunks.size(); i++) {
      RegisterWorkerPRequest chunk = requestChunks.get(i);
      requestObserver.onNext(chunk);
      // Signal after the 1st request has been sent
      if (i == 0) {
        latch.countDown();
      }
    }
    requestObserver.onCompleted();
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
    BlockMasterTestUtils.verifyBlockOnWorkers(
        mBlockMaster, newBlockId, newBlockSize, ImmutableList.of(workerInfo));
  }

  private static List<String> getTierAliases(Map<TierAlias, List<Integer>> tierConfig) {
    return tierConfig.keySet().stream().map(TierAlias::toString).collect(Collectors.toList());
  }

  // Verify a worker can reregister and have the correct final blocks
  private void verifyWorkerCanReregister(long workerId,
      List<RegisterWorkerPRequest> requestChunks, int expectedBlockCount) throws Exception {
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks,
        RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    assertEquals(errorQueue.toString(), 0, errorQueue.size());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(expectedBlockCount, worker.getBlockCount());
    assertEquals(0, worker.getToRemoveBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  private Command sendHeartbeatToMaster(long workerId) {
    return mBlockMaster.workerHeartbeat(
        workerId,
        CLUSTER_ID,
        CAPACITY_MAP,
        USAGE_MAP,
        // list of removed blockIds
        ImmutableList.of(),
        // list of added blockIds
        ImmutableMap.of(),
        NO_LOST_STORAGE,
        ImmutableList.of());
  }
}
