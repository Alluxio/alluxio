package alluxio.client.fs;

import alluxio.clock.ManualClock;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockInfoException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
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
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static alluxio.client.fs.RegisterStreamTestUtils.convert;
import static alluxio.client.fs.RegisterStreamTestUtils.getErrorCapturingResponseObserver;
import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.client.fs.RegisterStreamTestUtils.BATCH_SIZE;
import static alluxio.client.fs.RegisterStreamTestUtils.EMPTY_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.LOST_STORAGE;
import static alluxio.client.fs.RegisterStreamTestUtils.NET_ADDRESS_1;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_CONFIG;
import static alluxio.client.fs.RegisterStreamTestUtils.TIER_BLOCK_TOTAL;

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

  private static final long BLOCK_SIZE = ServerConfiguration.global().getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final Map<String, Long> MEM_CAPACITY = ImmutableMap.of("MEM", 100L);
  private static final Map<String, Long> MEM_USAGE_EMPTY = ImmutableMap.of("MEM", 0L);
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
    ServerConfiguration.set(PropertyKey.WORKER_REGISTER_STREAM_BATCH_SIZE, BATCH_SIZE);
    ServerConfiguration.set(PropertyKey.MASTER_WORKER_TIMEOUT_MS, MASTER_WORKER_TIMEOUT);

    // TODO(jiacheng): use a manual clock to better control
    ServerConfiguration.set(PropertyKey.MASTER_REGISTER_WORKER_STREAM_TIMEOUT, "3s");

    mRegistry = new MasterRegistry();
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mClock = new ManualClock();

    mExecutorService =
            Executors.newFixedThreadPool(10, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
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

  /**
   * Tests below cover the most normal cases.
   */
  @Test
  public void registerEmptyWorkerStream() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForEmptyWorker(workerId);

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
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

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
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
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
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));
    System.out.println("Re-register stream completed on client side");

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
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    // Verify the worker has registered
    assertEquals(1, mBlockMaster.getWorkerCount());

    // Register again
    System.out.println("Re-register now");
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));
    System.out.println("Re-register stream completed on client side");
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
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    // Verify the worker has registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL, worker.getBlockCount());
    assertEquals(0, worker.getToRemoveBlockCount());

    // Manually generate the blocks again and remove some
    List<String> tierAliases = getTierAliases(convert(TIER_CONFIG));
    Map<String, Long> capacityMap = Maps.toMap(tierAliases, (tier) -> CAPACITY);
    Map<String, Long> usedMap = Maps.toMap(tierAliases, (tier) -> 0L);
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(TIER_CONFIG));
    Set<Long> lostBlocks = removeSomeBlocks(blockMap);
    System.out.println(lostBlocks + " blocks went lost");
    // Regenerate the requests
    RegisterStreamer newRegisterStreamer = new RegisterStreamer(null,
            workerId, tierAliases, capacityMap, usedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    List<RegisterWorkerPRequest> newRequestChunks = ImmutableList.copyOf(newRegisterStreamer);
    int newExpectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL-lostBlocks.size())/(double)BATCH_SIZE);
    assertEquals(newExpectedBatchCount, newRequestChunks.size());

    // Register again with the updated stream
    System.out.println("Re-registering with added blocks");
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(newRequestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));
    assertEquals(0, newErrorQueue.size());

    // Verify the worker is registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo updatedWorker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL-lostBlocks.size(), updatedWorker.getBlockCount());
    // The master will mark the lost blocks as to be removed
    // This is to ensure the unrecognized blocks do no live on the worker anymore
    assertEquals(lostBlocks.size(), updatedWorker.getToRemoveBlockCount());

    // The update is received during the registration so no command to send to the worker
    // TODO(jiacheng): does this command make sense?
    Command command = mBlockMaster.workerHeartbeat(workerId,
        capacityMap,
        usedMap,
        // list of removed blockIds
        ImmutableList.of(),
        ImmutableMap.of(),
        NO_LOST_STORAGE,
        ImmutableList.of());
    System.out.println(command);
    assertEquals(CommandType.Free, command.getCommandType());
    assertEquals(lostBlocks.size(), command.getDataCount());

    // Verify the worker is readable and writable
    verifyWorkerWritable(workerId);
  }

  @Test
  public void registerExistingWorkerBlocksAdded() throws Exception {
    long workerId = mBlockMaster.getWorkerId(NET_ADDRESS_1);
    // Register the worker for the 1st time
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));
    assertEquals(0, errorQueue.size());
    // Verify the worker has registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL, worker.getBlockCount());
    assertEquals(0, worker.getToRemoveBlockCount());

    // Generate block IDs in the same way but add some more
    Map<BlockStoreLocation, List<Long>> blockMap =
        RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(TIER_CONFIG));
    Set<Long> addedBlocks = addSomeBlocks(blockMap);
    // Make the master accept these blocks
    prepareBlocksOnMaster(addedBlocks);

    // Re-generate the request
    List<String> tierAliases = getTierAliases(convert(TIER_CONFIG));
    Map<String, Long> capacityMap = Maps.toMap(tierAliases, (tier) -> CAPACITY);
    Map<String, Long> usedMap = Maps.toMap(tierAliases, (tier) -> 0L);
    System.out.println(addedBlocks + " blocks are added");
    RegisterStreamer newRegisterStreamer = new RegisterStreamer(null,
        workerId, tierAliases, capacityMap, usedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);
    List<RegisterWorkerPRequest> newRequestChunks = ImmutableList.copyOf(newRegisterStreamer);
    int newExpectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL+addedBlocks.size())/(double)BATCH_SIZE);
    assertEquals(newExpectedBatchCount, newRequestChunks.size());

    // Register again with the new request stream
    System.out.println("Re-registering with added blocks");
    Queue<Throwable> newErrorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(newRequestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(newErrorQueue));
    assertEquals(0, newErrorQueue.size());

    // Verify the worker is registered
    assertEquals(1, mBlockMaster.getWorkerCount());
    MasterWorkerInfo updatedWorker = mBlockMaster.getWorker(workerId);
    assertEquals(TIER_BLOCK_TOTAL+addedBlocks.size(), updatedWorker.getBlockCount());
    assertEquals(0, updatedWorker.getToRemoveBlockCount());

    // No command from the master because the update is received during registration
    Command command = mBlockMaster.workerHeartbeat(workerId,
            capacityMap,
            usedMap,
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

  /**
   * Tests below cover various failure cases.
   */
  @Test
  public void hangingWorkerSessionRecycled() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
        mHandler.registerWorkerStream(getErrorCapturingResponseObserver(errorQueue));

    // Feed the chunks into the requestObserver
    for (int i = 0; i < requestChunks.size(); i++) {
      RegisterWorkerPRequest chunk = requestChunks.get(i);
      requestObserver.onNext(chunk);
      // TODO(jiacheng): this time mgmt is terrible
      CommonUtils.sleepMs(2000);
      mClock.addTime(Duration.of(100_000_000, ChronoUnit.MILLIS));
    }

    // TODO(jiacheng): This should be rejected too
    requestObserver.onCompleted();
    System.out.println("Stream completed on client side");

    System.out.println(errorQueue);

    // Verify the session is recycled
  }

  @Test
  public void workerSendsErrorOnStart() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    StreamObserver<RegisterWorkerPRequest> requestObserver =
            mHandler.registerWorkerStream(RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    // Instead of sending requests to the master, the worker is interrupted
    // around the beginning of the stream. The error propagated to the master.
    Exception e = new InterruptedException("Worker is interrupted");
    requestObserver.onError(e);
    System.out.println("Worker sends error before the 1st request");

    // After the requestObserver.onError(), no requests
    // shall be accepted by the master.
    for (RegisterWorkerPRequest chunk : requestChunks) {
      requestObserver.onNext(chunk);
    }
    requestObserver.onCompleted();
    assertEquals(requestChunks.size() + 1, errorQueue.size());

    System.out.println(errorQueue);

    // verify the worker is not registered
    assertEquals(0, mBlockMaster.getWorkerCount());

    verifyWorkerCanReregister(workerId, requestChunks, 100+200+300+1000+1500+2000);
  }

  @Test
  public void workerSendsErrorInStream() throws Exception {
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

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
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);

    // Send the requests to the master
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
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getNoopResponseObserver());

    long blockToRemove = findFirstBlock(requestChunks);

    // Register again
    System.out.println("Register again");
    CountDownLatch latch = new CountDownLatch(1);
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    Future f = mExecutorService.submit(() -> {
      sendStreamToMasterAndSignal(requestChunks,
          RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue), latch);
      System.out.println("register finished");
    });

    // During the register stream, trigger a delete on worker
    System.out.println("waiting for the latch");
    latch.await();
    System.out.println("delete now");
    mBlockMaster.removeBlocks(ImmutableList.of(blockToRemove), true);
    System.out.println("Deleted");

    // Wait for the register to finish
    System.out.println("Waiting for the register to finish before the checks");
    f.get();
    System.out.println("Worker finished register");

    assertThrows(BlockInfoException.class, () -> {
      mBlockMaster.getBlockInfo(blockToRemove);
    });
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(1, mBlockMaster.getWorkerCount());
    assertEquals(100+200+300+1000+1500+2000-1, worker.getBlockCount());

    // TODO(jiacheng): usages are incorrect!
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
    Command command = mBlockMaster.workerHeartbeat(workerId,
            MEM_CAPACITY,
            MEM_USAGE_EMPTY,
            // list of removed blockIds
            ImmutableList.of(),
            ImmutableMap.of(),
            NO_LOST_STORAGE,
            ImmutableList.of());
    System.out.println(command);
    assertEquals(Command.newBuilder().addData(blockToRemove).setCommandType(CommandType.Free).build(), command);
  }

  @Test
  public void reregisterWithFree() throws Exception {
    // Register the worker so the worker is marked active in master
    long workerId = getWorkerId(NET_ADDRESS_1);
    List<RegisterWorkerPRequest> requestChunks = RegisterStreamTestUtils.generateRegisterStreamForWorker(workerId);
    prepareBlocksOnMaster(requestChunks);
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

  // TODO(jiacheng): migrate these utility functions
  public long getWorkerId(WorkerNetAddress address) throws Exception {
    long workerId = mBlockMaster.getWorkerId(address);
    System.out.println("Worker id " + workerId);
    return workerId;
  }

  public void prepareBlocksOnMaster(Collection<Long> blockIds) throws UnavailableException {
    for (long id : blockIds) {
      mBlockMaster.commitBlockInUFS(id, BLOCK_SIZE);
    }
  }

  public void prepareBlocksOnMaster(List<RegisterWorkerPRequest> requestChunks) throws Exception{
    for (RegisterWorkerPRequest chunk : requestChunks) {
      List<LocationBlockIdListEntry> entries = chunk.getCurrentBlocksList();
      for (LocationBlockIdListEntry entry : entries) {
        for (long blockId : entry.getValue().getBlockIdList()) {
          mBlockMaster.commitBlockInUFS(blockId, BLOCK_SIZE);
        }
      }
    }
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
      System.out.println("Sending batch " + i);
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
    BlockMasterTestUtils.verifyBlockOnWorkers(mBlockMaster, newBlockId, newBlockSize, ImmutableList.of(workerInfo));
  }

  private static List<String> getTierAliases(Map<TierAlias, List<Integer>> tierConfig) {
    return tierConfig.keySet().stream().map(TierAlias::toString).collect(Collectors.toList());
  }

  private void verifyWorkerCanReregister(long workerId, List<RegisterWorkerPRequest> requestChunks, int expectedBlockCount) throws Exception {
    Queue<Throwable> errorQueue = new ConcurrentLinkedQueue<>();
    sendStreamToMaster(requestChunks, RegisterStreamTestUtils.getErrorCapturingResponseObserver(errorQueue));

    assertEquals(0, errorQueue.size());
    MasterWorkerInfo worker = mBlockMaster.getWorker(workerId);
    assertEquals(expectedBlockCount, worker.getBlockCount());
    assertEquals(1, mBlockMaster.getWorkerCount());
  }

  private long findFirstBlock(List<RegisterWorkerPRequest> chunks) {
    RegisterWorkerPRequest firstBatch = chunks.get(0);
    LocationBlockIdListEntry entry = firstBatch.getCurrentBlocks(0);
    return entry.getValue().getBlockId(0);
  }
}
