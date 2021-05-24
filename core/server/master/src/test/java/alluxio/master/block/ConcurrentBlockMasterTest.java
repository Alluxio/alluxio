package alluxio.master.block;

import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.clock.ManualClock;
import alluxio.exception.BlockInfoException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.StorageList;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.meta.Block;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
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
  private static final long BLOCK1_ID = 1L;
  private static final long BLOCK1_LENGTH = 49L;
  private static final long BLOCK2_ID = 2L;
  private static final long BLOCK2_LENGTH = 59L;
  private static final Map<String, Long> MEM_CAPACITY = ImmutableMap.of("MEM", 100L);
  private static final Map<String, Long> MEM_USAGE_EMPTY = ImmutableMap.of("MEM", 0L);

  private BlockMaster mBlockMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;
  private ExecutorService mClientExecutorService;
  private MetricsMaster mMetricsMaster;
  private CoreMasterContext mMasterContext;

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
    mMasterContext = MasterTestUtils.testMasterContext();
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
    CountDownLatch readerLatch = new CountDownLatch(1);
    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, mMasterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), readerLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), MEM_CAPACITY, MEM_USAGE_EMPTY,
            NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());

    concurrentWriterWithReaders(
            readerLatch,
            // Writer
            () -> {
              testMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
              return null;
            },
            // Reader
            () -> {
              try {
                // If the block is not committed yet, a BlockInfoException will be thrown
                BlockInfo blockInfo = testMaster.getBlockInfo(BLOCK1_ID);
                List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());

                BlockLocation blockLocation = new BlockLocation()
                        .setTierAlias("MEM")
                        .setWorkerAddress(NET_ADDRESS_1)
                        .setWorkerId(worker1)
                        .setMediumType("MEM");
                BlockInfo expectedBlockInfo = new BlockInfo()
                        .setBlockId(BLOCK1_ID)
                        .setLength(BLOCK1_LENGTH)
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
                assertTrue(BLOCK1_LENGTH == worker.getUsedBytes() || 100L == worker.getUsedBytes());
              }
              return null;
            });
  }

  /**
   *  Concurrently run writer with a bunch of readers.
   *  The readers and the writer use a {@link CountDownLatch} for signal passing.
   *  The writer will release the latch in the middle or the write.
   *  The readers will wait on the latch and start to read on seeing the latch released.
   */
  private void concurrentWriterWithReaders(CountDownLatch readerLatch, Callable writer, Callable reader) throws Exception {
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
    // TODO(jiacheng): remove once all the tests are finished
    for (Throwable t : uncaughtThrowables) {
      t.printStackTrace();
    }
    assertEquals(0, uncaughtThrowables.size());
  }


  /**
   * RW contention
   * Concurrent remove operation and readers
   * Readers should read the state either before or after the removal
   * */
  @Test
  public void concurrentRemoveWithReaders() throws Exception {
    // Test remove operation where the metadata is deleted with blocks
    testRemoveWithReaders(true);
    // Test free operation where the metadata is not deleted
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
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), MEM_CAPACITY,
            MEM_USAGE_EMPTY, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());

    // Prepare blocks in master
    testMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM",
            BLOCK1_ID, BLOCK1_LENGTH);

    // Commit will send a signal but we don't want that signal
    // Readers will wait on the new latch
    CountDownLatch readerLatch = new CountDownLatch(1);
    testMaster.setLatch(readerLatch);

    concurrentWriterWithReaders(readerLatch,
      // Writer
      () -> {
        List<Long> blocksToRemove = new ArrayList<>();
        blocksToRemove.add(BLOCK1_ID);
        testMaster.removeBlocks(blocksToRemove, delete);
        return null;
      },
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
          verifyBlockOnWorkers(testMaster, BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);
        } catch (BlockInfoException e) {
          // If the block has been removed, this exception is expected
          // There is nothing more to test here
        }
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
  public void concurrentCommitWithRegisterNewWorkerSameBlock() throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    // To be replaced with the real latch W2 waits on
    CountDownLatch tempLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), tempLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    CountDownLatch w1Latch = new CountDownLatch(1);
    testMaster.setLatch(w1Latch);

    // A new worker as the W2
    long worker2 = testMaster.getWorkerId(NET_ADDRESS_2);
    concurrentWriterWithWriter(w1Latch,
            // W1
            () -> {
              testMaster.commitBlock(worker1, 49L, "MEM", "MEM",
                      BLOCK1_ID, BLOCK1_LENGTH);
              return null;
            },
            // W2
            () -> {
              // The new worker contains the block
              Block.BlockLocation blockLocation = Block.BlockLocation
                      .newBuilder().setTier("MEM")
                      .setMediumType("MEM")
                      .setWorkerId(worker2).build();
              List<Long> blockList = ImmutableList.of(BLOCK1_ID);
              // W1 will commit the block exclusively before worker2 registers with the same block.
              // So when worker 2 comes in, the block should be committed already.
              // So the block on worker 2 should be recognized.
              testMaster.workerRegister(worker2, Arrays.asList("MEM"),
                      MEM_CAPACITY,
                      ImmutableMap.of("MEM", BLOCK1_LENGTH),
                      ImmutableMap.of(blockLocation, blockList),
                      NO_LOST_STORAGE,
                      RegisterWorkerPOptions.getDefaultInstance());
              System.out.println("New worker register finished");
              return null;
            },
            // Verifier
            () -> {
              // After registration, verify the worker info
              List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
              assertEquals(2, workerInfoList.size());
              WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
              assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
              WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
              assertEquals(BLOCK1_LENGTH, worker2Info.getUsedBytes());

              verifyBlockOnWorkers(testMaster, BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);

              return null;
            });
  }

  @Test
  public void concurrentCommitWithRegisterNewWorkerDifferentBlock() throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    // To be replaced with the real latch W2 waits on
    CountDownLatch tempLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), tempLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    CountDownLatch w1Latch = new CountDownLatch(1);
    testMaster.setLatch(w1Latch);

    // A new worker as the W2
    long worker2 = testMaster.getWorkerId(NET_ADDRESS_2);
    concurrentWriterWithWriter(w1Latch,
            // W1
            () -> {
              testMaster.commitBlock(worker1, 49L, "MEM", "MEM",
                      BLOCK1_ID, BLOCK1_LENGTH);
              return null;
            },
            // W2
            () -> {
              // The new worker contains another block
              Block.BlockLocation blockLocation = Block.BlockLocation
                      .newBuilder().setTier("MEM")
                      .setMediumType("MEM")
                      .setWorkerId(worker2).build();
              List<Long> blockList = ImmutableList.of(BLOCK2_ID);
              // The new block on worker 2 is not recognized
              testMaster.workerRegister(worker2, Arrays.asList("MEM"),
                      MEM_CAPACITY,
                      ImmutableMap.of("MEM", BLOCK2_LENGTH),
                      ImmutableMap.of(blockLocation, blockList),
                      NO_LOST_STORAGE,
                      RegisterWorkerPOptions.getDefaultInstance());
              System.out.println("New worker register finished");
              return null;
            },
            // Verifier
            () -> {
              // After registration, verify the worker info
              List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
              assertEquals(2, workerInfoList.size());
              WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
              assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
              // Although the new block is not recognized, the worker usage will be taken as-is
              // That new block will be marked orphaned on the worker and will be removed later
              // So that later the worker usage will be rectified
              WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
              assertEquals(BLOCK2_LENGTH, worker2Info.getUsedBytes());

              // Verify the block metadata
              verifyBlockOnWorkers(testMaster, BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList(worker1Info));
              verifyBlockNotExisting(testMaster, BLOCK2_ID);
              return null;
            });
  }

  @Test
  public void concurrentCommitWithSameWorkerHeartbeatSameBlock() throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    // To be replaced with the real latch W2 waits on
    CountDownLatch tempLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), tempLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    CountDownLatch w1Latch = new CountDownLatch(1);
    testMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
            // W1
            () -> {
              testMaster.commitBlock(worker1, 49L, "MEM", "MEM",
                      BLOCK1_ID, BLOCK1_LENGTH);
              return null;
            },
            // W2
            () -> {
              // The same block is removed on worker in this heartbeat
              // This should succeed as commit locks the block exclusively and finishes first
              // When the block heartbeat processes the same block, it has been committed
              Command cmd = testMaster.workerHeartbeat(worker1,
                      MEM_CAPACITY,
                      // 0 used because the block is already removed
                      MEM_USAGE_EMPTY,
                      // list of removed blockIds
                      ImmutableList.of(BLOCK1_ID),
                      ImmutableMap.of(),
                      NO_LOST_STORAGE,
                      ImmutableList.of());
              System.out.println("worker heartbeat finished with command returned: " + cmd);

              // The block has been removed, nothing from command
              assertTrue(cmd.getCommandType().equals(CommandType.Nothing));

              return null;
            },
            // Verifier
            () -> {
              // After heartbeat, verify the worker info
              List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
              assertEquals(1, workerInfoList.size());
              WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
              assertEquals(0L, worker1Info.getUsedBytes());

              // The block has no locations now because the last location is removed
              verifyBlockOnWorkers(testMaster, BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList());

              return null;
            });
  }

  @Test
  public void concurrentCommitWithSameWorkerHeartbeatDifferentBlock() throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    // To be replaced with the real latch W2 waits on
    CountDownLatch tempLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), tempLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);

    // Register with block 2 on the worker
    Block.BlockLocation block2Location = Block.BlockLocation
            .newBuilder().setTier("MEM")
            .setMediumType("MEM")
            .setWorkerId(worker1).build();
    List<Long> existingBlocks = ImmutableList.of(BLOCK2_ID);
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), MEM_CAPACITY,
            ImmutableMap.of("MEM", BLOCK2_LENGTH),
            ImmutableMap.of(block2Location, existingBlocks),
            NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    CountDownLatch w1Latch = new CountDownLatch(1);
    testMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
            // W1
            () -> {
              testMaster.commitBlock(worker1, 49L, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
              return null;
            },
            // W2
            () -> {
              // A different block is removed on the same worker
              // This should contend on the worker metadata
              Command cmd = testMaster.workerHeartbeat(worker1,
                      MEM_CAPACITY,
                      // 0 used because the block is already removed
                      MEM_USAGE_EMPTY,
                      // list of removed blockIds
                      ImmutableList.of(BLOCK2_ID),
                      ImmutableMap.of(),
                      NO_LOST_STORAGE,
                      ImmutableList.of());
              System.out.println("worker heartbeat finished with command returned: " + cmd);

              // The block has been removed, nothing from command
              assertTrue(cmd.getCommandType().equals(CommandType.Nothing));

              return null;
            },
            // Verifier
            () -> {
              // After heartbeat, verify the worker info
              List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
              assertEquals(1, workerInfoList.size());
              WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
              assertEquals(0L, worker1Info.getUsedBytes());

              verifyBlockOnWorkers(testMaster, BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);
              return null;
            });
  }

  @Test
  public void concurrentCommitWithDifferentWorkerHeartbeatSameBlock() throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    // To be replaced with the real latch W2 waits on
    CountDownLatch tempLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), tempLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = testMaster.getWorkerId(NET_ADDRESS_2);
    long block1Id = 1L;
    long block1Length = 49L;
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    // The block is on worker 2
    Block.BlockLocation block2Location = Block.BlockLocation
            .newBuilder().setTier("MEM")
            .setMediumType("MEM")
            .setWorkerId(worker2).build();
    List<Long> existingBlocks = ImmutableList.of(block1Id);
    testMaster.workerRegister(worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", block1Length),
            ImmutableMap.of(block2Location, existingBlocks),
            NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    CountDownLatch w1Latch = new CountDownLatch(1);
    testMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
            // W1
            () -> {
              testMaster.commitBlock(worker1, 49L, "MEM", "MEM", block1Id, block1Length);
              return null;
            },
            // W2
            () -> {
              // The same block is removed on another worker
              // This should succeed as commit locks the block exclusively and finishes first
              // When the block heartbeat processes the same block, it has been committed
              Command cmd = testMaster.workerHeartbeat(worker2,
                      MEM_CAPACITY,
                      // 0 used because the block is already removed
                      MEM_USAGE_EMPTY,
                      // list of removed blockIds
                      ImmutableList.of(block1Id),
                      ImmutableMap.of(),
                      NO_LOST_STORAGE,
                      ImmutableList.of());
              System.out.println("worker heartbeat finished with command returned: " + cmd);

              // The block has been removed, nothing from command
              assertTrue(cmd.getCommandType().equals(CommandType.Nothing));

              return null;
            },
            // Verifier
            () -> {
              // After heartbeat, verify the worker info
              List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
              assertEquals(2, workerInfoList.size());
              WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
              assertEquals(block1Length, worker1Info.getUsedBytes());
              WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
              assertEquals(0L, worker2Info.getUsedBytes());

              // The block has 1 location on worker 1
              verifyBlockOnWorkers(testMaster, block1Id, block1Length, Arrays.asList(worker1Info));

              return null;
            });
  }

  private Block.BlockLocation createBlockOnWorkerMemTier(long workerId) {
    return Block.BlockLocation
            .newBuilder().setTier("MEM")
            .setMediumType("MEM")
            .setWorkerId(workerId).build();
  }

  @Test
  // TODO(jiacheng)
  public void concurrentCommitWithDifferentWorkerHeartbeatDifferentBlock() throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    // To be replaced with the real latch W2 waits on
    CountDownLatch tempLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), tempLatch);

    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    long worker2 = testMaster.getWorkerId(NET_ADDRESS_2);

    // Register with block 2 on both worker
    // Block 2 exists in alluxio but not on these 2 workers
    testMaster.commitBlockInUFS(BLOCK2_ID, BLOCK2_LENGTH);

    Block.BlockLocation block2Worker1Location = Block.BlockLocation
            .newBuilder().setTier("MEM")
            .setMediumType("MEM")
            .setWorkerId(worker1).build();
    List<Long> existingBlocks = ImmutableList.of(BLOCK2_ID);
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", BLOCK2_LENGTH),
            ImmutableMap.of(block2Worker1Location, existingBlocks),
            NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());

    // TODO(jiacheng): extract this
    Block.BlockLocation block2Worker2Location = Block.BlockLocation
            .newBuilder().setTier("MEM")
            .setMediumType("MEM")
            .setWorkerId(worker2).build();
    testMaster.workerRegister(worker2, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", BLOCK2_LENGTH),
            ImmutableMap.of(block2Worker2Location, existingBlocks),
            NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    CountDownLatch w1Latch = new CountDownLatch(1);
    testMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
            // W1
            () -> {
              testMaster.commitBlock(worker1, BLOCK1_LENGTH + BLOCK2_LENGTH,
                      "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
              return null;
            },
            // W2
            () -> {
              // A different block is removed on the same worker
              // This should contend on the worker metadata
              Command cmd = testMaster.workerHeartbeat(worker2,
                      MEM_CAPACITY,
                      // 0 used because the block is already removed
                      MEM_USAGE_EMPTY,
                      // list of removed blockIds
                      ImmutableList.of(BLOCK2_ID),
                      ImmutableMap.of(),
                      NO_LOST_STORAGE,
                      ImmutableList.of());
              System.out.println("worker heartbeat finished with command returned: " + cmd);

              // The block has been removed, nothing from command
              assertTrue(cmd.getCommandType().equals(CommandType.Nothing));

              return null;
            },
            // Verifier
            () -> {
              // After heartbeat, verify the worker info
              List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
              assertEquals(2, workerInfoList.size());
              WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
              assertEquals(BLOCK1_LENGTH + BLOCK2_LENGTH, worker1Info.getUsedBytes());
              WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
              assertEquals(0L, worker2Info.getUsedBytes());

              // Block 1 should exist on master 1
              verifyBlockOnWorkers(testMaster, BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList(worker1Info));

              // Block 2 should exist on master 1
              BlockInfo block2Info = testMaster.getBlockInfo(BLOCK2_ID);
              System.out.println("Found block 2 BlockInfo " + block2Info);
              // The block has no locations now because the last location is removed
              verifyBlockOnWorkers(testMaster, BLOCK2_ID, BLOCK2_LENGTH, Arrays.asList(worker1Info));
              return null;
            });
  }

  @Test
  // TODO(jiacheng)
  public void concurrentRemoveWithRegisterNewWorkerSameBlock() throws Exception {
    boolean deleteMetadata = false;

    CoreMasterContext masterContext = MasterTestUtils.testMasterContext();
    // To be replaced with the real latch W2 waits on
    CountDownLatch tempLatch = new CountDownLatch(1);

    SignalBlockMaster testMaster = new SignalBlockMaster(mMetricsMaster, masterContext, mClock,
            ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), tempLatch);

    long blockId = 1L;
    long blockLength = 49L;
    // Prepare worker
    long worker1 = testMaster.getWorkerId(NET_ADDRESS_1);
    testMaster.workerRegister(worker1, Arrays.asList("MEM"), ImmutableMap.of("MEM", 100L),
            ImmutableMap.of("MEM", 0L), NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
            RegisterWorkerPOptions.getDefaultInstance());
    // Prepare block on the worker
    testMaster.commitBlock(worker1, blockLength, "MEM", "MEM", blockId, blockLength);
    CountDownLatch w1Latch = new CountDownLatch(1);
    testMaster.setLatch(w1Latch);

    // A new worker as the W2
    long worker2 = testMaster.getWorkerId(NET_ADDRESS_2);
    concurrentWriterWithWriter(w1Latch,
            // W1
            () -> {
              // TODO(jiacheng): true?
              testMaster.removeBlocks(ImmutableList.of(blockId), false);
              return null;
            },
            // W2
            () -> {
              // The new worker contains the block
              Block.BlockLocation blockLocation = Block.BlockLocation
                      .newBuilder().setTier("MEM")
                      .setMediumType("MEM")
                      .setWorkerId(worker2).build();
              List<Long> blockList = ImmutableList.of(blockId);
              // W1 will remove the block exclusively before worker2 registers with the same block
              // So when worker 2 comes in, the block should be removed already
              // So the block on worker 2 should be ignored
              testMaster.workerRegister(worker2, Arrays.asList("MEM"),
                      ImmutableMap.of("MEM", 100L),
                      ImmutableMap.of("MEM", blockLength),
                      ImmutableMap.of(blockLocation, blockList),
                      NO_LOST_STORAGE,
                      RegisterWorkerPOptions.getDefaultInstance());
              System.out.println("New worker register finished");
              return null;
            },
            // Verifier
            () -> {
              // After registration, verify the worker info
              List<WorkerInfo> workerInfoList = testMaster.getWorkerReport(GetWorkerReportOptions.defaults());
              assertEquals(2, workerInfoList.size());
              WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
              assertEquals(blockLength, worker1Info.getUsedBytes());
              WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
              assertEquals(blockLength, worker2Info.getUsedBytes());

              // Verify the block metadata
              if (!deleteMetadata) {
                // The master will issue commands to remove blocks on the next heartbeat
                // So now the locations are still there
                verifyBlockOnWorkers(testMaster, blockId, blockLength, workerInfoList);
              } else {
                // If the block metadata has been removed, getting that will get an exception
                assertThrows(BlockInfoException.class, () -> {
                  testMaster.getBlockInfo(blockId);
                });
              }

              // Verify the heartbeat from worker will get a command to remove the block
              Command worker1HeartbeatCmd = testMaster.workerHeartbeat(worker1,
                      ImmutableMap.of("MEM", 100L),
                      // the block has not yet been removed
                      ImmutableMap.of("MEM", blockLength),
                      // an empty list of removed blockIds
                      ImmutableList.of(),
                      ImmutableMap.of(),
                      NO_LOST_STORAGE,
                      ImmutableList.of());
              System.out.println("Worker 1 heartbeat gets " + worker1HeartbeatCmd);
              Command expectedCmd = Command.newBuilder().setCommandType(CommandType.Free).addData(1).build();
              assertEquals(expectedCmd, worker1HeartbeatCmd);

              Command worker2HeartbeatCmd = testMaster.workerHeartbeat(worker2,
                      ImmutableMap.of("MEM", 100L),
                      // the block has not yet been removed
                      ImmutableMap.of("MEM", blockLength),
                      // an empty list of removed blockIds
                      ImmutableList.of(),
                      ImmutableMap.of(),
                      NO_LOST_STORAGE,
                      ImmutableList.of());
              System.out.println("Worker 2 heartbeat gets " + worker2HeartbeatCmd);
              // Block on worker 2 will not be freed because worker 2 registered after the free
              assertTrue(worker2HeartbeatCmd.getCommandType().equals(CommandType.Nothing));

              return null;
            });
  }

  @Test
  // TODO(jiacheng)
  public void concurrentRemoveWithRegisterNewWorkerDifferentBlock() throws Exception {
  }

  @Test
  // TODO(jiacheng)
  public void concurrentRemoveWithSameWorkerHeartbeatSameBlock() throws Exception {
  }

  @Test
  // TODO(jiacheng)
  public void concurrentRemoveWithSameWorkerHeartbeatDifferentBlock() throws Exception {
  }

  @Test
  // TODO(jiacheng)
  public void concurrentRemoveWithDifferentWorkerHeartbeatSameBlock() throws Exception {
  }

  @Test
  // TODO(jiacheng)
  public void concurrentRemoveWithDifferentWorkerHeartbeatDifferentBlock() throws Exception {
  }

  /**
   * Verifies the {@link BlockInfo} including the length and locations
   *
   * @param blockMaster the block master that is running
   * @param blockId the target block id
   * @param blockLength the block should have this length
   * @param workers the block should be on these workers
   */
  private void verifyBlockOnWorkers(SignalBlockMaster blockMaster, long blockId, long blockLength,
                                    List<WorkerInfo> workers) throws Exception {
    BlockInfo blockInfo = blockMaster.getBlockInfo(blockId);
    System.out.println("Found BlockInfo " + blockInfo);
    assertEquals(blockLength, blockInfo.getLength());
    assertEquals(workers.size(), blockInfo.getLocations().size());

    List<BlockLocation> expectedLocations = new ArrayList<>();
    for (WorkerInfo w : workers) {
      expectedLocations.add(new BlockLocation()
              .setWorkerAddress(w.getAddress())
              .setWorkerId(w.getId())
              .setMediumType("MEM")
              .setTierAlias("MEM"));
    }

    assertEquals(blockLength, blockInfo.getLength());
    assertEquals(expectedLocations.size(), blockInfo.getLocations().size());
    assertEquals(new HashSet<>(expectedLocations), new HashSet<>(blockInfo.getLocations()));
  }

  private void verifyBlockNotExisting(SignalBlockMaster blockMaster, long blockId) {
    assertThrows(BlockInfoException.class, () -> {
      blockMaster.getBlockInfo(blockId);
    });
  }

  private WorkerInfo findWorkerInfo(List<WorkerInfo> list, long workerId) {
    for (WorkerInfo worker : list) {
      if (workerId == worker.getId()) {
        return worker;
      }
    }
    throw new AssertionError(String.format("Failed to find workerId %s in the worker list %s", workerId, list));
  }

  /**
   * Concurrently runs two writers W1 and W2, and verifies the state after W1 and W2 are complete.
   * W1 runs first, in the middle of execution W1 will release the {@link CountDownLatch}.
   * W2 will be waiting for the latch and then runs the writer operation.
   * The verifier will run last verifying the final state.
   *
   * @param w1Latch W1 will trigger this latch, W2 will wait on this latch
   * @param w1 writer 1
   * @param w2 writer 2
   * @param verifier the verifier of the final state
   */
  private void concurrentWriterWithWriter(CountDownLatch w1Latch,
                                          Callable w1,
                                          Callable w2,
                                          Callable verifier) throws Exception {
    // This thread count is intentionally larger than the client thread pool
    // In the hope that even if the first batch of clients all read the state before commit really happens
    // The following batch will capture the state after the commit
    Queue<Throwable> uncaughtThrowables = new ConcurrentLinkedQueue<>();
    CountDownLatch writerFinished = new CountDownLatch(1);

    mClientExecutorService.submit(() -> {
      // Wait until the writer enters the critical section and sends a signal
      try {
        w1Latch.await();
        System.out.println("Writer 2 can run now");
      } catch (Throwable t) {
        System.out.println("Error waiting for W1: " + t.getMessage());
        uncaughtThrowables.add(t);
        // Fail to wait for the signal, just give up
        writerFinished.countDown();
        return;
      }
      // Trigger the other writer
      try {
        System.out.println("Calling W2");
        w2.call();
      } catch (Throwable t) {
        System.out.println("W2 throws an error: " + t.getMessage());
        uncaughtThrowables.add(t);
      } finally {
        writerFinished.countDown();
      }
    });

    // Call W1 in the same thread
    w1.call();

    writerFinished.await();
    verifier.call();
    // W2 has finished, verify here
    // If any assertion failed, the failed assertion will throw an AssertError
    // TODO(jiacheng): remove this
    for (Throwable t : uncaughtThrowables) {
      t.printStackTrace();
    }
    assertEquals(0, uncaughtThrowables.size());
  }
}
