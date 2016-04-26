/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.worker.WorkerContext;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link BlockLockManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMetadataManager.class)
public class BlockLockManagerTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;

  private BlockLockManager mLockManager;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the test fails
   */
  @Before
  public void before() throws Exception {
    BlockMetadataManager mockMetaManager = PowerMockito.mock(BlockMetadataManager.class);
    PowerMockito.when(mockMetaManager.hasBlockMeta(TEST_BLOCK_ID)).thenReturn(true);
    mLockManager = new BlockLockManager();
  }

  @After
  public void after() throws Exception {
    WorkerContext.reset();
  }

  /**
   * Tests the {@link BlockLockManager#lockBlock(long, long, BlockLockType)} method.
   */
  @Test
  public void lockBlockTest() {
    // Read-lock on can both get through
    long lockId1 = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    Assert.assertNotEquals(lockId1, lockId2);
  }

  /**
   * Tests that an exception is thrown when trying to unlock a block via
   * {@link BlockLockManager#unlockBlock(long)} which is not locked.
   *
   * @throws Exception if unlocking the block fails
   */
  @Test
  public void unlockNonExistingLockTest() throws Exception {
    long badLockId = 1;
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(badLockId));
    // Unlock a non-existing lockId, expect to see IOException
    mLockManager.unlockBlock(badLockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} which is not locked.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void validateLockIdWithNoRecordTest() throws Exception {
    long badLockId = 1;
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(badLockId));
    // Validate a non-existing lockId, expect to see IOException
    mLockManager.validateLock(TEST_SESSION_ID, TEST_BLOCK_ID, badLockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} with an incorrect session ID.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void validateLockIdWithWrongSessionIdTest() throws Exception {
    long lockId = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long wrongSessionId = TEST_SESSION_ID + 1;
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_SESSION.getMessage(lockId,
        TEST_SESSION_ID, wrongSessionId));
    // Validate an existing lockId with wrong session id, expect to see IOException
    mLockManager.validateLock(wrongSessionId, TEST_BLOCK_ID, lockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} with an incorrect block ID.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void validateLockIdWithWrongBlockIdTest() throws Exception {
    long lockId = mLockManager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    long wrongBlockId = TEST_BLOCK_ID + 1;
    mThrown.expect(InvalidWorkerStateException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_BLOCK.getMessage(lockId,
        TEST_BLOCK_ID, wrongBlockId));
    // Validate an existing lockId with wrong block id, expect to see IOException
    mLockManager.validateLock(TEST_SESSION_ID, wrongBlockId, lockId);
  }

  /**
   * Tests that an exception is thrown when trying to validate a lock of a block via
   * {@link BlockLockManager#validateLock(long, long, long)} after the session was cleaned up.
   *
   * @throws Exception if the validation of the lock fails
   */
  @Test
  public void cleanupSessionTest() throws Exception {
    long sessionId1 = TEST_SESSION_ID;
    long sessionId2 = TEST_SESSION_ID + 1;
    long lockId1 = mLockManager.lockBlock(sessionId1, TEST_BLOCK_ID, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(sessionId2, TEST_BLOCK_ID, BlockLockType.READ);
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(lockId2));
    mLockManager.cleanupSession(sessionId2);
    // Expect validating sessionId1 to get through
    mLockManager.validateLock(sessionId1, TEST_BLOCK_ID, lockId1);
    // Because sessionId2 has been cleaned up, expect validating sessionId2 to throw IOException
    mLockManager.validateLock(sessionId2, TEST_BLOCK_ID, lockId2);
  }

  /**
   * Tests that up to WORKER_TIERED_STORE_BLOCK_LOCKS block locks can be grabbed simultaneously.
   */
  @Test(timeout = 10000)
  public void grabManyLocksTest() throws Exception {
    int maxLocks = 100;
    setMaxLocks(maxLocks);
    BlockLockManager manager = new BlockLockManager();
    for (int i = 0; i < maxLocks; i++) {
      manager.lockBlock(i, i, BlockLockType.WRITE);
    }
    lockExpectingHang(manager, 101);
  }

  /**
   * Tests that two sessions can both take a read lock on the same block.
   */
  @Test(timeout = 10000)
  public void lockAcrossSessionsTest() throws Exception {
    BlockLockManager manager = new BlockLockManager();
    manager.lockBlock(1, TEST_BLOCK_ID, BlockLockType.READ);
    manager.lockBlock(2, TEST_BLOCK_ID, BlockLockType.READ);
  }

  /**
   * Tests that a write lock can't be taken while a read lock is held.
   */
  @Test(timeout = 10000)
  public void readBlocksWriteTest() throws Exception {
    BlockLockManager manager = new BlockLockManager();
    manager.lockBlock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    lockExpectingHang(manager, TEST_BLOCK_ID);
  }

  /**
   * Tests that block locks are returned to the pool when they are no longer in use.
   */
  @Test(timeout = 10000)
  public void reuseLockTest() throws Exception {
    setMaxLocks(1);
    BlockLockManager manager = new BlockLockManager();
    long lockId1 = manager.lockBlock(TEST_SESSION_ID, 1, BlockLockType.WRITE);
    manager.unlockBlock(lockId1); // Without this line the next lock would hang.
    manager.lockBlock(TEST_SESSION_ID, 2, BlockLockType.WRITE);
  }

  /**
   * Tests that block locks are not returned to the pool when they are still in use.
   */
  @Test(timeout = 10000)
  public void dontReuseLockTest() throws Exception {
    setMaxLocks(1);
    final BlockLockManager manager = new BlockLockManager();
    long lockId1 = manager.lockBlock(TEST_SESSION_ID, 1, BlockLockType.READ);
    manager.lockBlock(TEST_SESSION_ID, 1, BlockLockType.READ);
    manager.unlockBlock(lockId1);
    lockExpectingHang(manager, 2);
  }

  /**
   * Calls {@link BlockLockManager#lockBlock(long, long, BlockLockType)} and fails if it doesn't
   * hang.
   *
   * @param the block id to try locking
   */
  private void lockExpectingHang(final BlockLockManager manager, final long blockId)
      throws Exception {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        manager.lockBlock(TEST_SESSION_ID, blockId, BlockLockType.WRITE);
      }
    });
    thread.start();
    thread.join(200);
    // Locking should not take 200ms unless there is a hang.
    Assert.assertTrue(thread.isAlive());
  }

  /**
   * Tests that taking and releasing many block locks concurrently won't cause a failure.
   *
   * This is done by creating 200 threads, 100 for each of 2 different block ids. Each thread locks
   * and then unlocks its block 50 times. After this, it takes a final lock on its block before
   * returning. At the end of the test, the internal state of the lock manager is validated.
   */
  @Test(timeout = 10000)
  public void stressTest() throws Throwable {
    final int numBlocks = 2;
    final int threadsPerBlock = 100;
    final int lockUnlocksPerThread = 50;
    setMaxLocks(numBlocks);
    final BlockLockManager manager = new BlockLockManager();
    final List<Thread> threads = Lists.newArrayList();
    final CyclicBarrier barrier = new CyclicBarrier(numBlocks * threadsPerBlock);
    // If there are exceptions, we will store them here.
    final AtomicReference<List<Throwable>> failedThreadThrowables =
        new AtomicReference<List<Throwable>>(Lists.<Throwable>newArrayList());
    Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        failedThreadThrowables.get().add(ex);
      }
    };
    for (int blockId = 0; blockId < numBlocks; blockId++) {
      final int finalBlockId = blockId;
      for (int i = 0; i < threadsPerBlock; i++) {
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
            // Lock and unlock the block lockUnlocksPerThread times.
            for (int j = 0; j < lockUnlocksPerThread; j++) {
              long lockId = manager.lockBlock(TEST_SESSION_ID, finalBlockId, BlockLockType.READ);
              try {
                manager.unlockBlock(lockId);
              } catch (BlockDoesNotExistException e) {
                throw Throwables.propagate(e);
              }
            }
            // Lock the block one last time.
            manager.lockBlock(TEST_SESSION_ID, finalBlockId, BlockLockType.READ);
          }
        });
        t.setUncaughtExceptionHandler(exceptionHandler);
        threads.add(t);
      }
    }
    Collections.shuffle(threads);
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    if (!failedThreadThrowables.get().isEmpty()) {
      StringBuilder sb = new StringBuilder("Failed with the following errors:\n");
      for (Throwable failedThreadThrowable : failedThreadThrowables.get()) {
        sb.append(Throwables.getStackTraceAsString(failedThreadThrowable));
      }
      Assert.fail(sb.toString());
    }
    manager.validate();
  }

  private void setMaxLocks(int maxLocks) {
    WorkerContext.getConf().set(Constants.WORKER_TIERED_STORE_BLOCK_LOCKS,
        Integer.toString(maxLocks));
  }
}
