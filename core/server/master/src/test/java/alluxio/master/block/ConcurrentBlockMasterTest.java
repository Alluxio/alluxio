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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentBlockMasterTest {
  private static final WorkerNetAddress NET_ADDRESS_1 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(80).setDataPort(81).setWebPort(82);
  private static final WorkerNetAddress NET_ADDRESS_2 = new WorkerNetAddress().setHost("localhost")
      .setRpcPort(83).setDataPort(84).setWebPort(85);

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

  private SignalBlockMaster mBlockMaster;
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
    mMasterContext = MasterTestUtils.testMasterContext();
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, mMasterContext);
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    mClientExecutorService = Executors.newFixedThreadPool(2,
        ThreadFactoryUtils.build("TestBlockMaster-%d", true));
    // No one is listening to this latch, so do any preparation for the test
    CountDownLatch voidLatch = new CountDownLatch(1);
    mBlockMaster = new SignalBlockMaster(mMetricsMaster, mMasterContext, mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), voidLatch);
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
   * RW contention: Concurrent commit and readers.
   * Signal in commit and the readers inquire the state
   */
  @Test
  public void concurrentCommitWithReaders() throws Exception {
    // Prepare worker
    long worker1 = registerEmptyWorker(NET_ADDRESS_1);
    // Replace the latch used for preparation
    CountDownLatch readerLatch = new CountDownLatch(1);
    mBlockMaster.setLatch(readerLatch);

    concurrentWriterWithReaders(
        readerLatch,
        // Writer
        () -> {
          mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
          return null;
        },
        // Reader
        () -> {
          try {
            // If the block is not committed yet, a BlockInfoException will be thrown
            BlockInfo blockInfo = mBlockMaster.getBlockInfo(BLOCK1_ID);
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
            WorkerInfo worker = findWorkerInfo(workerInfoList, worker1);
            assertEquals(BLOCK1_LENGTH, worker.getUsedBytes());

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
          } catch (BlockInfoException e) {
            // The reader came in before the writer started the commit
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
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
   * RW contention: Concurrent remove operation and readers.
   * Readers should read the state either before or after the removal.
   */
  @Test
  public void concurrentRemoveWithReaders() throws Exception {
    for (boolean deleteMetadata : ImmutableList.of(true, false)) {
      // Prepare worker and block 1 on worker
      long worker1 = registerEmptyWorker(NET_ADDRESS_1);
      mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM",
          BLOCK1_ID, BLOCK1_LENGTH);

      CountDownLatch readerLatch = new CountDownLatch(1);
      mBlockMaster.setLatch(readerLatch);

      concurrentWriterWithReaders(readerLatch,
          // Writer
          () -> {
            List<Long> blocksToRemove = new ArrayList<>();
            blocksToRemove.add(BLOCK1_ID);
            mBlockMaster.removeBlocks(blocksToRemove, deleteMetadata);
            return null;
          },
          // Reader
          () -> {
            try {
              // Even if the block is removed, the worker usage will not be updated
              // until the next worker heartbeat
              List<WorkerInfo> workerInfoList = mBlockMaster.getWorkerReport(
                  GetWorkerReportOptions.defaults());
              assertEquals(1, workerInfoList.size());
              WorkerInfo worker = workerInfoList.get(0);
              assertEquals(BLOCK1_LENGTH, worker.getUsedBytes());

              // If the block is removed already, a BlockInfoException will be thrown
              verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);
            } catch (BlockInfoException e) {
              // If the block has been removed, this exception is expected
              // There is nothing more to test here
            }
            return null;
          });
    }
  }

  /**
   *  Concurrently run writer with a bunch of readers.
   *  The readers and the writer use a {@link CountDownLatch} for signal passing.
   *  The writer will release the latch in the middle or the write.
   *  The readers will wait on the latch and start to read on seeing the latch released.
   *
   * @param readerLatch the latch that writer releases and readers wait on
   * @param writer the writer function, nothing will be returned
   * @param reader the reader function, nothing will be returned
   */
  private void concurrentWriterWithReaders(CountDownLatch readerLatch,
                                           Callable writer, Callable reader) throws Exception {
    // This thread count is intentionally larger than the client thread pool
    // In the hope that even if the first batch of clients all read the state before
    // the commit really happens
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
    assertEquals(0, uncaughtThrowables.size());
  }

  /**
   * Tests on WW contentions.
   *
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
   */
  @Test
  public void concurrentCommitWithRegisterNewWorkerSameBlock() throws Exception {
    // Prepare worker
    long worker1 = registerEmptyWorker(NET_ADDRESS_1);
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    CountDownLatch w1Latch = new CountDownLatch(1);
    mBlockMaster.setLatch(w1Latch);
    concurrentWriterWithWriter(w1Latch,
        // W1
        () -> {
          mBlockMaster.commitBlock(worker1, 49L, "MEM", "MEM",
              BLOCK1_ID, BLOCK1_LENGTH);
          return null;
        },
        // W2
        () -> {
          // W1 will commit the block exclusively before worker 2 registers with the same block
          // So when worker 2 comes in, the block should be committed already
          // So the block on worker 2 should be recognized
          mBlockMaster.workerRegister(worker2, Arrays.asList("MEM"),
              MEM_CAPACITY,
              ImmutableMap.of("MEM", BLOCK1_LENGTH),
              ImmutableMap.of(newBlockLocationOnWorkerMemTier(worker2),
                  ImmutableList.of(BLOCK1_ID)),
              NO_LOST_STORAGE,
              RegisterWorkerPOptions.getDefaultInstance());
          return null;
        },
        // Verifier
        () -> {
          // After registration, verify the worker info
          List<WorkerInfo> workerInfoList = mBlockMaster.getWorkerReport(
              GetWorkerReportOptions.defaults());
          assertEquals(2, workerInfoList.size());
          WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
          assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
          WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
          assertEquals(BLOCK1_LENGTH, worker2Info.getUsedBytes());

          verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);

          return null;
        });
  }

  @Test
  public void concurrentCommitWithRegisterNewWorkerDifferentBlock() throws Exception {
    // Prepare worker
    long worker1 = registerEmptyWorker(NET_ADDRESS_1);
    long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
    CountDownLatch w1Latch = new CountDownLatch(1);
    mBlockMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
        // W1
        () -> {
          mBlockMaster.commitBlock(worker1, 49L, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
          return null;
        },
        // W2
        () -> {
          // The new worker contains another block
          // The new block on worker 2 is not recognized and will be ignored by master
          // because the block metadata is not in alluxio
          mBlockMaster.workerRegister(worker2, Arrays.asList("MEM"),
              MEM_CAPACITY,
              ImmutableMap.of("MEM", BLOCK2_LENGTH),
              ImmutableMap.of(newBlockLocationOnWorkerMemTier(worker2),
                  ImmutableList.of(BLOCK2_ID)),
              NO_LOST_STORAGE,
              RegisterWorkerPOptions.getDefaultInstance());
          return null;
        },
        // Verifier
        () -> {
          // After registration, verify the worker info
          List<WorkerInfo> workerInfoList = mBlockMaster
              .getWorkerReport(GetWorkerReportOptions.defaults());
          assertEquals(2, workerInfoList.size());
          WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
          assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
          // Although the new block is not recognized, the worker usage will be taken as-is
          // That new block will be marked orphaned on the worker and will be removed later
          // So that later the worker usage will be rectified
          WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
          assertEquals(BLOCK2_LENGTH, worker2Info.getUsedBytes());

          // Verify the block metadata
          verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList(worker1Info));
          verifyBlockNotExisting(BLOCK2_ID);
          return null;
        });
  }

  @Test
  public void concurrentCommitWithSameWorkerHeartbeatSameBlock() throws Exception {
    // Prepare worker
    long worker1 = registerEmptyWorker(NET_ADDRESS_1);
    CountDownLatch w1Latch = new CountDownLatch(1);
    mBlockMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
        // W1
        () -> {
          mBlockMaster.commitBlock(worker1, 49L, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
          return null;
        },
        // W2
        () -> {
          // The same block is removed on worker in this heartbeat
          // This should succeed as commit locks the block exclusively and finishes first
          // When the block heartbeat processes the same block, it has been committed
          Command cmd = mBlockMaster.workerHeartbeat(worker1,
              MEM_CAPACITY,
              // 0 used because the block removed on this worker
              MEM_USAGE_EMPTY,
              // list of removed blockIds
              ImmutableList.of(BLOCK1_ID),
              ImmutableMap.of(),
              NO_LOST_STORAGE,
              ImmutableList.of());

          // The block has been removed, nothing from command
          assertEquals(EMPTY_CMD, cmd);

          return null;
        },
        // Verifier
        () -> {
          // After heartbeat, verify the worker info
          List<WorkerInfo> workerInfoList = mBlockMaster.getWorkerReport(
              GetWorkerReportOptions.defaults());
          assertEquals(1, workerInfoList.size());
          WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
          assertEquals(0L, worker1Info.getUsedBytes());

          // The block has no locations now because the last location is removed
          verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList());

          return null;
        });
  }

  @Test
  public void concurrentCommitWithSameWorkerHeartbeatDifferentBlock() throws Exception {
    // Prepare worker
    long worker1 = registerEmptyWorker(NET_ADDRESS_1);
    // Block 2 is on worker 1
    mBlockMaster.commitBlock(worker1, BLOCK2_LENGTH, "MEM", "MEM", BLOCK2_ID, BLOCK2_LENGTH);
    CountDownLatch w1Latch = new CountDownLatch(1);
    mBlockMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
        // W1
        () -> {
          mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
          return null;
        },
        // W2
        () -> {
          // A different block is removed on the same worker
          // This should contend on the worker metadata
          Command cmd = mBlockMaster.workerHeartbeat(worker1,
              MEM_CAPACITY,
              // 0 used because the block is already removed
              MEM_USAGE_EMPTY,
              // list of removed blockIds
              ImmutableList.of(BLOCK2_ID),
              ImmutableMap.of(),
              NO_LOST_STORAGE,
              ImmutableList.of());

          // The block has been removed, nothing from command
          assertEquals(EMPTY_CMD, cmd);

          return null;
        },
        // Verifier
        () -> {
          // After heartbeat, verify the worker info
          List<WorkerInfo> workerInfoList = mBlockMaster.getWorkerReport(
              GetWorkerReportOptions.defaults());
          assertEquals(1, workerInfoList.size());
          WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
          assertEquals(0L, worker1Info.getUsedBytes());

          verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);
          return null;
        });
  }

  @Test
  public void concurrentCommitWithDifferentWorkerHeartbeatSameBlock() throws Exception {
    // Prepare worker
    long worker1 = registerEmptyWorker(NET_ADDRESS_1);
    long worker2 = registerEmptyWorker(NET_ADDRESS_2);
    // The block is on worker 2
    mBlockMaster.commitBlock(worker2, BLOCK2_LENGTH, "MEM", "MEM", BLOCK2_ID, BLOCK2_LENGTH);

    CountDownLatch w1Latch = new CountDownLatch(1);
    mBlockMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
        // W1
        () -> {
          mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
          return null;
        },
        // W2
        () -> {
          // The same block is removed on another worker
          // This should succeed as commit locks the block exclusively and finishes first
          // When the block heartbeat processes the same block, it has been committed
          Command cmd = mBlockMaster.workerHeartbeat(worker2,
              MEM_CAPACITY,
              // 0 used because the block is already removed
              MEM_USAGE_EMPTY,
              // list of removed blockIds
              ImmutableList.of(BLOCK1_ID),
              ImmutableMap.of(),
              NO_LOST_STORAGE,
              ImmutableList.of());

          // The block has been removed, nothing from command
          assertEquals(EMPTY_CMD, cmd);

          return null;
        },
        // Verifier
        () -> {
          // After heartbeat, verify the worker info
          List<WorkerInfo> workerInfoList = mBlockMaster
              .getWorkerReport(GetWorkerReportOptions.defaults());
          assertEquals(2, workerInfoList.size());
          WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
          assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
          WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
          assertEquals(0L, worker2Info.getUsedBytes());

          // The block has 1 location on worker 1
          verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList(worker1Info));

          return null;
        });
  }

  private Block.BlockLocation newBlockLocationOnWorkerMemTier(long workerId) {
    return Block.BlockLocation.newBuilder().setTier("MEM").setMediumType("MEM")
        .setWorkerId(workerId).build();
  }

  @Test
  public void concurrentCommitWithDifferentWorkerHeartbeatDifferentBlock() throws Exception {
    // Prepare worker
    long worker1 = registerEmptyWorker(NET_ADDRESS_1);
    long worker2 = registerEmptyWorker(NET_ADDRESS_2);
    // Register with block 2 on both workers
    mBlockMaster.commitBlock(worker1, BLOCK2_LENGTH, "MEM", "MEM", BLOCK2_ID, BLOCK2_LENGTH);
    mBlockMaster.commitBlock(worker2, BLOCK2_LENGTH, "MEM", "MEM", BLOCK2_ID, BLOCK2_LENGTH);

    CountDownLatch w1Latch = new CountDownLatch(1);
    mBlockMaster.setLatch(w1Latch);

    concurrentWriterWithWriter(w1Latch,
        // W1
        () -> {
          // worker 1 has block 1 and block 2 now
          mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH + BLOCK2_LENGTH,
              "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
          return null;
        },
        // W2
        () -> {
          // A different block is removed on the same worker
          // This should contend on the worker metadata
          Command cmd = mBlockMaster.workerHeartbeat(worker2,
              MEM_CAPACITY,
              // 0 used because the block is already removed
              MEM_USAGE_EMPTY,
              // list of removed blockIds
              ImmutableList.of(BLOCK2_ID),
              ImmutableMap.of(),
              NO_LOST_STORAGE,
              ImmutableList.of());

          // The block has been removed, nothing from command
          assertEquals(EMPTY_CMD, cmd);

          return null;
        },
        // Verifier
        () -> {
          // After heartbeat, verify the worker info
          List<WorkerInfo> workerInfoList = mBlockMaster
              .getWorkerReport(GetWorkerReportOptions.defaults());
          assertEquals(2, workerInfoList.size());
          WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
          assertEquals(BLOCK1_LENGTH + BLOCK2_LENGTH, worker1Info.getUsedBytes());
          WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
          assertEquals(0L, worker2Info.getUsedBytes());

          // Block 1 should exist on master 1
          verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList(worker1Info));

          // Block 2 should exist on master 1
          verifyBlockOnWorkers(BLOCK2_ID, BLOCK2_LENGTH, Arrays.asList(worker1Info));
          return null;
        });
  }

  @Test
  public void concurrentRemoveWithRegisterNewWorkerSameBlock() throws Exception {
    for (boolean deleteMetadata : ImmutableList.of(true, false)) {
      // Prepare worker
      long worker1 = registerEmptyWorker(NET_ADDRESS_1);
      // Prepare block on the worker
      mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
      CountDownLatch w1Latch = new CountDownLatch(1);
      mBlockMaster.setLatch(w1Latch);

      // A new worker as the W2
      long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
      concurrentWriterWithWriter(w1Latch,
          // W1
          () -> {
            mBlockMaster.removeBlocks(ImmutableList.of(BLOCK1_ID), deleteMetadata);
            return null;
          },
          // W2
          () -> {
            // The new worker contains the block
            // W1 will remove the block exclusively before worker2 registers with the same block
            // So when worker 2 comes in, the block should be removed already
            // So the block on worker 2 should be ignored
            mBlockMaster.workerRegister(worker2, Arrays.asList("MEM"),
                MEM_CAPACITY,
                ImmutableMap.of("MEM", BLOCK1_LENGTH),
                ImmutableMap.of(newBlockLocationOnWorkerMemTier(worker2),
                    ImmutableList.of(BLOCK1_ID)),
                NO_LOST_STORAGE,
                RegisterWorkerPOptions.getDefaultInstance());
            return null;
          },
          // Verifier
          () -> {
            // After registration, verify the worker info
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
            assertEquals(2, workerInfoList.size());
            WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
            assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
            WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
            assertEquals(BLOCK1_LENGTH, worker2Info.getUsedBytes());

            // Verify the block metadata
            if (deleteMetadata) {
              // If the block metadata has been removed, getting that will get an exception
              assertThrows(BlockInfoException.class, () -> {
                mBlockMaster.getBlockInfo(BLOCK1_ID);
              });
            } else {
              // The master will issue commands to remove blocks on the next heartbeat
              // So now the locations are still there
              verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);
            }

            // Verify the heartbeat from worker will get a command to remove the block
            Command worker1HeartbeatCmd = mBlockMaster.workerHeartbeat(worker1,
                MEM_CAPACITY,
                // the block has not yet been removed
                ImmutableMap.of("MEM", BLOCK1_LENGTH),
                // an empty list of removed blockIds
                ImmutableList.of(),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());
            assertEquals(FREE_BLOCK1_CMD, worker1HeartbeatCmd);

            if (deleteMetadata) {
              // Block on worker 2 will be freed because the block is already removed
              Command worker2HeartbeatCmd = mBlockMaster.workerHeartbeat(worker2,
                  MEM_CAPACITY,
                  // the block has not yet been removed
                  ImmutableMap.of("MEM", BLOCK1_LENGTH),
                  // an empty list of removed blockIds
                  ImmutableList.of(),
                  ImmutableMap.of(),
                  NO_LOST_STORAGE,
                  ImmutableList.of());
              // Block on worker 2 will be freed because the block is already removed
              // Unrecognized blocks will be freed
              assertEquals(FREE_BLOCK1_CMD, worker2HeartbeatCmd);
            } else {
              // Two cases can happen:
              // 1. Worker 2 registers before the free operation checks the block locations
              //    In this case the block on worker 2 will be freed
              // 2. Worker 2 registers after the free operation is complete
              //    In this case the block on worker 2 will not be freed
              Command worker2HeartbeatCmd = mBlockMaster.workerHeartbeat(worker2,
                  MEM_CAPACITY,
                  // the block has not yet been removed
                  ImmutableMap.of("MEM", BLOCK1_LENGTH),
                  // an empty list of removed blockIds
                  ImmutableList.of(),
                  ImmutableMap.of(),
                  NO_LOST_STORAGE,
                  ImmutableList.of());
              assertTrue(worker2HeartbeatCmd.equals(FREE_BLOCK1_CMD)
                  || worker2HeartbeatCmd.equals(EMPTY_CMD));
            }

            return null;
          });
    }
  }

  @Test
  public void concurrentRemoveWithRegisterNewWorkerDifferentBlock() throws Exception {
    for (boolean deleteMetadata : ImmutableList.of(true, false)) {
      // Prepare worker
      long worker1 = registerEmptyWorker(NET_ADDRESS_1);
      // Prepare block on the worker
      mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
      // Prepare block 2 so it is recognized at worker register
      mBlockMaster.commitBlockInUFS(BLOCK2_ID, BLOCK2_LENGTH);
      CountDownLatch w1Latch = new CountDownLatch(1);
      mBlockMaster.setLatch(w1Latch);

      // A new worker as the W2
      long worker2 = mBlockMaster.getWorkerId(NET_ADDRESS_2);
      concurrentWriterWithWriter(w1Latch,
          // W1
          () -> {
            mBlockMaster.removeBlocks(ImmutableList.of(BLOCK1_ID), deleteMetadata);
            return null;
          },
          // W2
          () -> {
            // The new worker contains the block
            // W1 will remove the block exclusively before worker2 registers with the same block
            // So when worker 2 comes in, the block should be removed already
            // So the block on worker 2 should be ignored
            mBlockMaster.workerRegister(worker2, Arrays.asList("MEM"),
                MEM_CAPACITY,
                ImmutableMap.of("MEM", BLOCK2_LENGTH),
                ImmutableMap.of(newBlockLocationOnWorkerMemTier(worker2),
                    ImmutableList.of(BLOCK2_ID)),
                NO_LOST_STORAGE,
                RegisterWorkerPOptions.getDefaultInstance());
            return null;
          },
          // Verifier
          () -> {
            // After registration, verify the worker info
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
            assertEquals(2, workerInfoList.size());
            // Block 1 has not been removed from the workers yet
            WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
            assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
            WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
            assertEquals(BLOCK2_LENGTH, worker2Info.getUsedBytes());

            // Verify the block metadata
            if (deleteMetadata) {
              verifyBlockNotExisting(BLOCK1_ID);
            } else {
              // The master will issue commands to remove blocks on the next heartbeat
              // So now the locations are still there
              verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, ImmutableList.of(worker1Info));
            }
            // Block 2 is unaffected
            verifyBlockOnWorkers(BLOCK2_ID, BLOCK2_LENGTH, ImmutableList.of(worker2Info));

            // Regardless of whether the metadata is removed, the existing block will be freed
            Command worker1HeartbeatCmd = mBlockMaster.workerHeartbeat(worker1,
                MEM_CAPACITY,
                // the block has not yet been removed
                ImmutableMap.of("MEM", BLOCK1_LENGTH),
                // an empty list of removed blockIds
                ImmutableList.of(),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());
            assertEquals(FREE_BLOCK1_CMD, worker1HeartbeatCmd);

            Command worker2HeartbeatCmd = mBlockMaster.workerHeartbeat(worker2,
                MEM_CAPACITY,
                // the block has not yet been removed
                ImmutableMap.of("MEM", BLOCK1_LENGTH),
                // an empty list of removed blockIds
                ImmutableList.of(),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());
            // Blocks on worker 2 are unaffected
            assertEquals(EMPTY_CMD, worker2HeartbeatCmd);
            return null;
          });
    }
  }

  @Test
  public void concurrentRemoveWithSameWorkerHeartbeatSameBlock() throws Exception {
    for (boolean deleteMetadata : ImmutableList.of(true, false)) {
      // Prepare worker
      long worker1 = registerEmptyWorker(NET_ADDRESS_1);
      // Prepare block in alluxio
      mBlockMaster.commitBlockInUFS(BLOCK1_ID, BLOCK1_LENGTH);
      CountDownLatch w1Latch = new CountDownLatch(1);
      mBlockMaster.setLatch(w1Latch);

      concurrentWriterWithWriter(w1Latch,
          // W1
          () -> {
            mBlockMaster.removeBlocks(ImmutableList.of(BLOCK1_ID), deleteMetadata);
            return null;
          },
          // W2
          () -> {
            // The same block is removed on worker in this heartbeat
            // This should succeed as commit locks the block exclusively and finishes first
            // When the block heartbeat processes the same block, it has been committed
            Command cmd = mBlockMaster.workerHeartbeat(worker1,
                MEM_CAPACITY,
                // 0 used because the block is already removed
                MEM_USAGE_EMPTY,
                // list of removed blockIds
                ImmutableList.of(BLOCK1_ID),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());

            // The block has been removed, nothing from command
            assertEquals(EMPTY_CMD, cmd);

            return null;
          },
          // Verifier
          () -> {
            // After heartbeat, verify the worker info
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
            assertEquals(1, workerInfoList.size());
            WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
            assertEquals(0L, worker1Info.getUsedBytes());

            if (deleteMetadata) {
              verifyBlockNotExisting(BLOCK1_ID);
            } else {
              // The block has no locations now because the last location is removed
              verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList());
            }

            return null;
          });
    }
  }

  private long registerEmptyWorker(WorkerNetAddress address) throws Exception {
    long workerId = mBlockMaster.getWorkerId(address);
    mBlockMaster.workerRegister(workerId, Arrays.asList("MEM"), MEM_CAPACITY,
        MEM_USAGE_EMPTY, NO_BLOCKS_ON_LOCATION, NO_LOST_STORAGE,
        RegisterWorkerPOptions.getDefaultInstance());
    return workerId;
  }

  @Test
  public void concurrentRemoveWithSameWorkerHeartbeatDifferentBlock() throws Exception {
    for (boolean deleteMetadata : ImmutableList.of(true)) {
      // Prepare block 1 and 2 on the worker
      long worker1 = registerEmptyWorker(NET_ADDRESS_1);
      mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
      mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH + BLOCK2_LENGTH, "MEM", "MEM",
          BLOCK2_ID, BLOCK2_LENGTH);
      CountDownLatch w1Latch = new CountDownLatch(1);
      mBlockMaster.setLatch(w1Latch);

      AtomicBoolean freeCommandSeen = new AtomicBoolean(false);
      concurrentWriterWithWriter(w1Latch,
          // W1
          () -> {
            mBlockMaster.removeBlocks(ImmutableList.of(BLOCK1_ID), deleteMetadata);
            return null;
          },
          // W2
          () -> {
            // A different block is removed on the same worker
            // This should contend on the worker metadata
            Command cmd = mBlockMaster.workerHeartbeat(worker1,
                MEM_CAPACITY,
                // Block 2 is removed but 1 is still on the worker
                ImmutableMap.of("MEM", BLOCK1_LENGTH),
                // list of removed blockIds
                ImmutableList.of(BLOCK2_ID),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());

            // The heartbeat contends on the block lock of block 2, worker usage lock and
            // worker block list lock
            // The remove operation will first remove the block metadata with the block lock,
            // then update the worker to-be-removed list with the block list lock
            // There are two possible outcomes:
            // 1. Remove gets the block list lock first and updates the to-be-removed list
            //    In this case the returned value will be a free command.
            // 2. Worker heartbeat gets the block list lock first before the remove operation
            //    adds to the to-be-removed list. In this case the return command has nothing.
            if (cmd.equals(FREE_BLOCK1_CMD)) {
              freeCommandSeen.set(true);
            } else {
              assertEquals(EMPTY_CMD, cmd);
            }

            return null;
          },
          // Verifier
          () -> {
            // After heartbeat, verify the worker info
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
            assertEquals(1, workerInfoList.size());
            WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
            assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());

            if (deleteMetadata) {
              verifyBlockNotExisting(BLOCK1_ID);
            } else {
              // All locations of block 1 are freed in metadata
              verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, workerInfoList);
            }
            verifyBlockOnWorkers(BLOCK2_ID, BLOCK2_LENGTH, ImmutableList.of());

            // If the 1st heartbeat does not see the free command
            // This heartbeat should definitely see it,
            // because the verifier is run after W1 fully finished
            // and updated the to-be-removed list
            if (!freeCommandSeen.get()) {
              Command cmd = mBlockMaster.workerHeartbeat(worker1,
                  MEM_CAPACITY,
                  // Block 2 is removed but 1 is still on the worker
                  ImmutableMap.of("MEM", BLOCK1_LENGTH),
                  // list of removed blockIds
                  ImmutableList.of(BLOCK2_ID),
                  ImmutableMap.of(),
                  NO_LOST_STORAGE,
                  ImmutableList.of());
              assertEquals(FREE_BLOCK1_CMD, cmd);
            }
            return null;
          });
    }
  }

  @Test
  public void concurrentRemoveWithDifferentWorkerHeartbeatSameBlock() throws Exception {
    for (boolean deleteMetadata : ImmutableList.of(true, false)) {
      // Prepare worker
      long worker1 = registerEmptyWorker(NET_ADDRESS_1);
      long worker2 = registerEmptyWorker(NET_ADDRESS_2);
      mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
      mBlockMaster.commitBlock(worker2, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
      CountDownLatch w1Latch = new CountDownLatch(1);
      mBlockMaster.setLatch(w1Latch);

      concurrentWriterWithWriter(w1Latch,
          // W1
          () -> {
            mBlockMaster.removeBlocks(ImmutableList.of(BLOCK1_ID), deleteMetadata);
            return null;
          },
          // W2
          () -> {
            // The same block is removed on another worker
            // This should succeed as commit locks the block exclusively and finishes first
            // When the block heartbeat processes the same block, it has been committed
            Command cmd = mBlockMaster.workerHeartbeat(worker2,
                MEM_CAPACITY,
                // 0 used because the block is already removed
                MEM_USAGE_EMPTY,
                // list of removed blockIds
                ImmutableList.of(BLOCK1_ID),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());

            // The block has been removed, nothing from command
            assertEquals(EMPTY_CMD, cmd);

            return null;
          },
          // Verifier
          () -> {
            // After heartbeat, verify the worker info
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
            assertEquals(2, workerInfoList.size());
            // The block is still on worker 1, will be removed on the next heartbeat
            WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
            assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
            WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
            assertEquals(0L, worker2Info.getUsedBytes());

            if (deleteMetadata) {
              verifyBlockNotExisting(BLOCK1_ID);
            } else {
              // The location is still on worker 1, until it is removed after the next heartbeat
              verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, ImmutableList.of(worker1Info));
            }

            // On the heartbeat worker 1 block will be removed
            Command cmd = mBlockMaster.workerHeartbeat(worker1,
                MEM_CAPACITY,
                // Block 1 is still on worker 1
                ImmutableMap.of("MEM", BLOCK1_LENGTH),
                // list of removed blockIds
                ImmutableList.of(),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());
            assertEquals(FREE_BLOCK1_CMD, cmd);
            return null;
          });
    }
  }

  @Test
  public void concurrentRemoveWithDifferentWorkerHeartbeatDifferentBlock() throws Exception {
    for (boolean deleteMetadata : ImmutableList.of(true, false)) {
      // Prepare worker
      long worker1 = registerEmptyWorker(NET_ADDRESS_1);
      long worker2 = registerEmptyWorker(NET_ADDRESS_2);
      // Worker 1 has block 1
      mBlockMaster.commitBlock(worker1, BLOCK1_LENGTH, "MEM", "MEM", BLOCK1_ID, BLOCK1_LENGTH);
      // Worker 2 has block 2
      mBlockMaster.commitBlock(worker2, BLOCK2_LENGTH, "MEM", "MEM", BLOCK2_ID, BLOCK2_LENGTH);

      CountDownLatch w1Latch = new CountDownLatch(1);
      mBlockMaster.setLatch(w1Latch);

      concurrentWriterWithWriter(w1Latch,
          // W1
          () -> {
            mBlockMaster.removeBlocks(ImmutableList.of(BLOCK1_ID), deleteMetadata);
            return null;
          },
          // W2
          () -> {
            // A different block is removed on another worker
            Command cmd = mBlockMaster.workerHeartbeat(worker2,
                MEM_CAPACITY,
                // 0 used because the block is already removed
                MEM_USAGE_EMPTY,
                // list of removed blockIds
                ImmutableList.of(BLOCK2_ID),
                ImmutableMap.of(),
                NO_LOST_STORAGE,
                ImmutableList.of());

            // Nothing for worker 2 to do because it does not have block 1
            assertEquals(EMPTY_CMD, cmd);

            return null;
          },
          // Verifier
          () -> {
            // After heartbeat, verify the worker info
            List<WorkerInfo> workerInfoList = mBlockMaster
                .getWorkerReport(GetWorkerReportOptions.defaults());
            assertEquals(2, workerInfoList.size());
            WorkerInfo worker1Info = findWorkerInfo(workerInfoList, worker1);
            assertEquals(BLOCK1_LENGTH, worker1Info.getUsedBytes());
            WorkerInfo worker2Info = findWorkerInfo(workerInfoList, worker2);
            assertEquals(0L, worker2Info.getUsedBytes());

            if (deleteMetadata) {
              verifyBlockNotExisting(BLOCK1_ID);
            } else {
              // Block 1 should still exist on worker 1 until the next heartbeat frees it
              verifyBlockOnWorkers(BLOCK1_ID, BLOCK1_LENGTH, Arrays.asList(worker1Info));
            }

            // No copies for block 2
            verifyBlockOnWorkers(BLOCK2_ID, BLOCK2_LENGTH, Arrays.asList());
            return null;
          });
    }
  }

  /**
   * Verifies the {@link BlockInfo} including the length and locations.
   *
   * @param blockId the target block id
   * @param blockLength the block should have this length
   * @param workers the block should be on these workers
   */
  private void verifyBlockOnWorkers(long blockId, long blockLength,
                                    List<WorkerInfo> workers) throws Exception {
    BlockInfo blockInfo = mBlockMaster.getBlockInfo(blockId);
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

  private void verifyBlockNotExisting(long blockId) {
    assertThrows(BlockInfoException.class, () -> {
      mBlockMaster.getBlockInfo(blockId);
    });
  }

  private WorkerInfo findWorkerInfo(List<WorkerInfo> list, long workerId) {
    for (WorkerInfo worker : list) {
      if (workerId == worker.getId()) {
        return worker;
      }
    }
    throw new AssertionError(String.format(
        "Failed to find workerId %s in the worker list %s", workerId, list));
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
    // In the hope that even if the first batch of clients all read the state before
    // the commit really happens
    // The following batch will capture the state after the commit
    Queue<Throwable> uncaughtThrowables = new ConcurrentLinkedQueue<>();
    CountDownLatch writerFinished = new CountDownLatch(1);

    mClientExecutorService.submit(() -> {
      // Wait until the writer enters the critical section and sends a signal
      try {
        w1Latch.await();
      } catch (Throwable t) {
        uncaughtThrowables.add(t);
        // Fail to wait for the signal, just give up
        writerFinished.countDown();
        return;
      }
      // Trigger the other writer
      try {
        w2.call();
      } catch (Throwable t) {
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
    assertEquals(0, uncaughtThrowables.size());
  }
}
