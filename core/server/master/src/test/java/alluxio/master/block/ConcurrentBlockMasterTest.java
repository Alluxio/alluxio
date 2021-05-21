package alluxio.master.block;

import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.clock.ManualClock;
import alluxio.exception.BlockInfoException;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.StorageList;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConcurrentBlockMasterTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
          .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
          .setRpcPort(83).setDataPort(84).setWebPort(85);

  private static final List<Long> NO_BLOCKS = ImmutableList.of();
  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATION
          = ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final Block.BlockLocation BLOCK_LOCATION = Block.BlockLocation.newBuilder()
          .setTier("MEM").setMediumType("MEM").build();
  private static final int CONCURRENT_CLIENT_COUNT = 20;
  private static final long TEST_BLOCK_ID = 1L;
  private static final long TEST_BLOCK_LENGTH = 49L;

  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private ExecutorService mClientExecutorService;
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


  /**
   * RW contention
   * Concurrent commit and readers
   * Signal in commit and the readers inquire the state
   * */
  @Test
  public void concurrentCommitWithReaders() throws Exception {
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

    concurrentWriterWithReaders(testMaster, blockId, worker1, blockLength,
            readerLatch, () -> {
              try {
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
                List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
                assertEquals(1, workerInfoList.size());
                WorkerInfo worker = workerInfoList.get(0);
                // We may just see the result before or after the commit
                // But other values should be illegal
                assertTrue(49L == worker.getUsedBytes() || 100L == worker.getUsedBytes());
              }
              return null;
            }, () -> {
              testMaster.commitBlock(worker1, 49L, "MEM", "MEM", blockId, blockLength);
              return null;
            });
  }

  // TODO(jiacheng): comments
  private void concurrentWriterWithReaders(SignalBlockMaster blockMaster,
                                           Long blockId, Long workerId,
                                           Long blockLength,
                                           CountDownLatch readerLatch,
                                           Callable reader,
                                           Callable writer) throws Exception {
    // This thread count is intentionally larger than the client thread pool
    // In the hope that even if the first batch of clients all read the state before commit really happens
    // The following batch will capture the state after the commit
    Queue<Throwable> uncaughtThrowables = new ConcurrentLinkedQueue<>();
    CountDownLatch allClientFinished = new CountDownLatch(CONCURRENT_CLIENT_COUNT);
    for (int i = 0; i < CONCURRENT_CLIENT_COUNT; i++) {
      mClientExecutorService.submit(() -> {
        // Wait until the writer enters the critical section and sends a signal
        try {
          readerLatch.await();
        } catch (Throwable t) {
          uncaughtThrowables.add(t);
          // Fail to wait for the signal, just give up
          allClientFinished.countDown();
          return;
        }
        // Trigger the reader
        try {
          reader.call();
        } catch (Throwable t) {
          System.out.println("Reader throws an error: " + t.getMessage());
          uncaughtThrowables.add(t);
        } finally {
          allClientFinished.countDown();
        }
      });
    }

    // The readers should be waiting for the writer to send the signal
    writer.call();

    allClientFinished.await();
    // If any assertion failed, the failed assertion will throw an AssertError
    // TODO(jiacheng): remove this
    for (Throwable t : uncaughtThrowables) {
      t.printStackTrace();
    }
    assertEquals(0, uncaughtThrowables.size());
  }


  /**
   * RW contention
   * Concurrent remove and readers
   * Readers should read the state either before or after the removal
   * */
  @Test
  public void concurrentRemoveWithReaders() throws Exception {
    // Test remove
    testRemoveWithReaders(true);
    // Test free
    testRemoveWithReaders(false);
  }

  /**
   * @param delete when true, delete the metadata too
   * */
  private void testRemoveWithReaders(boolean delete) throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    CountDownLatch voidLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), voidLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = TEST_BLOCK_ID;
    long blockLength = TEST_BLOCK_LENGTH;
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());

    // Prepare blocks in master
    testMaster.commitBlock(worker1, 49L, "MEM", "MEM", blockId, blockLength);

    // Commit will send a signal but we don't want that signal
    // Readers will wait on the new latch
    CountDownLatch readerLatch = new CountDownLatch(1);
    testMaster.setLatch(readerLatch);

    concurrentWriterWithReaders(testMaster, blockId, worker1, blockLength, readerLatch,
            // Reader
            () -> {
              try {
                // Even if the block is removed, the worker usage will not be updated
                // until the next worker heartbeat
                List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
                assertEquals(1, workerInfoList.size());
                WorkerInfo worker = workerInfoList.get(0);
                assertEquals(49L, worker.getUsedBytes());

                // If the block is removed already, a BlockInfoException will be thrown
                // So the below assert will not be tested
                BlockInfo blockInfo = testMaster.getBlockInfo(blockId);
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
              } catch (BlockInfoException e) {
                // If the block has been removed, this exception is expected
                // There is nothing more to test here
              }
              return null;
            },
            // Writer
            () -> {
              List<Long> blocksToRemove = new ArrayList<>();
              blocksToRemove.add(blockId);
              testMaster.removeBlocks(blocksToRemove, delete);
              return null;
            });
  }

  /**
   * WW contention
   * Write operations are:
   * 1. commit
   * 2. remove
   * 3. workerRegister
   * 4. workerHeartbeat
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
  public void concurrentCommitWithRegister() throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    CountDownLatch w1Latch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), w1Latch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    long blockId = 1L;
    long blockLength = 49L;
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());

    concurrentWriterWithWriter(testMaster, blockId, worker1, blockLength, w1Latch,
            // W1
            () -> {
              testMaster.commitBlock(worker1, 49L, "MEM", "MEM", blockId, blockLength);
              return null;
            },
            // W2
            () -> {
              long worker2 = testMaster.getWorkerId(NET_ADDRESS_2);
              // The new worker contains the block
              Block.BlockLocation blockLocation = Block.BlockLocation
                      .newBuilder().setTier("MEM")
                      .setMediumType("MEM")
                      .setWorkerId(worker2).build();

              List<Long> blockList = ImmutableList.of(blockId);
              testMaster.workerRegister(worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
                      ImmutableMap.of("MEM", 0L),
                      ImmutableMap.of(blockLocation, blockList),
                      NO_LOST_STORAGE,
                      RegisterWorkerPOptions.getDefaultInstance());
              return null;
            });
    // TODO(jiacheng): what to verify here??
  }

  // TODO(jiacheng): comments
  private void concurrentWriterWithWriter(SignalBlockMaster blockMaster,
                                          Long blockId, Long workerId,
                                          Long blockLength,
                                          CountDownLatch w1Latch,
                                          Callable w1,
                                          Callable w2) throws Exception {
    // This thread count is intentionally larger than the client thread pool
    // In the hope that even if the first batch of clients all read the state before commit really happens
    // The following batch will capture the state after the commit
    Queue<Throwable> uncaughtThrowables = new ConcurrentLinkedQueue<>();
    CountDownLatch writerFinished = new CountDownLatch(1);

    mClientExecutorService.submit(() -> {
      // Wait until the writer enters the critical section and sends a signal
      try {
        w1Latch.await();
      } catch (Throwable t) {
        System.out.println("Error waiting for W1: " + t.getMessage());
        uncaughtThrowables.add(t);
        // Fail to wait for the signal, just give up
        writerFinished.countDown();
        return;
      }
      // Trigger the other writer
      try {
        w2.call();
      } catch (Throwable t) {
        System.out.println("W2 throws an error: " + t.getMessage());
        uncaughtThrowables.add(t);
      } finally {
        writerFinished.countDown();
      }
    });
  }
}
