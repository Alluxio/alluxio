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

package alluxio.worker.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.base.Throwables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link BlockLockManager}.
 */
public final class BlockLockManagerTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_SESSION_ID2 = 3;
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
   */
  @Before
  public void before() {
    mLockManager = new BlockLockManager();
  }

  @After
  public void after() {
    Configuration.reloadProperties();
  }

  /**
   * Tests the {@link BlockLockManager#acquireBlockLock(long, long, BlockLockType)} method.
   */
  @Test
  public void lockBlock() {
    // Read-lock on can both get through
    BlockLock lockId1 =
        mLockManager.acquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    BlockLock lockId2 =
        mLockManager.acquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID, BlockLockType.READ);
    assertNotEquals(lockId1.get(), lockId2.get());
  }

  @Test
  public void tryLockBlock() {
    // Read-lock on can both get through
    Optional<BlockLock> lock1 = mLockManager.tryAcquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID,
        BlockLockType.READ, 1, TimeUnit.MINUTES);
    Optional<BlockLock> lock2 = mLockManager.tryAcquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID,
        BlockLockType.READ, 1, TimeUnit.MINUTES);
    assertNotEquals(lock1.get().get(), lock2.get().get());
  }

  @Test
  public void tryLockBlockWithReadLockLocked() {
    // Hold a read-lock on test block
    try (BlockLock lock = mLockManager.acquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID,
        BlockLockType.READ)) {
      // Read-lock on the same block should get through
      Optional<BlockLock> lock2 = mLockManager.tryAcquireBlockLock(TEST_SESSION_ID2, TEST_BLOCK_ID,
          BlockLockType.READ, 1, TimeUnit.SECONDS);
      assertTrue(lock2.isPresent());
      lock2.get().close();
      // Write-lock should fail
      Optional<BlockLock> lock3 = mLockManager.tryAcquireBlockLock(TEST_SESSION_ID2, TEST_BLOCK_ID,
          BlockLockType.WRITE, 1, TimeUnit.SECONDS);
      assertEquals(Optional.empty(), lock3);
    }
  }

  @Test
  public void tryLockBlockWithWriteLockLocked() {
    // Hold a write-lock on test block
    try (BlockLock lock = mLockManager.acquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID,
        BlockLockType.WRITE)) {
      // Read-lock should fail
      Optional<BlockLock> lockId2 =
          mLockManager.tryAcquireBlockLock(TEST_SESSION_ID2, TEST_BLOCK_ID,
              BlockLockType.READ, 1, TimeUnit.SECONDS);
      assertFalse(lockId2.isPresent());
      // Write-lock should fail
      Optional<BlockLock> lockId3 =
          mLockManager.tryAcquireBlockLock(TEST_SESSION_ID2, TEST_BLOCK_ID,
              BlockLockType.WRITE, 1, TimeUnit.SECONDS);
      assertFalse(lockId3.isPresent());
    }
  }

  /**
   * Test when trying to validate a lock of a block via
   * {@link BlockLockManager#checkLock(long, long, long)} which is not locked.
   */
  @Test
  public void validateLockIdWithNoRecord() {
    long badLockId = 1;
    assertFalse(mLockManager.checkLock(TEST_SESSION_ID, TEST_BLOCK_ID, badLockId));
  }

  /**
   * Test when trying to validate a lock of a block via
   * {@link BlockLockManager#checkLock(long, long, long)} with an incorrect session ID.
   */
  @Test
  public void validateLockIdWithWrongSessionId() {
    try (BlockLock lock = mLockManager.acquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID,
        BlockLockType.READ)) {
      long wrongSessionId = TEST_SESSION_ID + 1;
      // Validate an existing lockId with wrong session id, expect to see IOException
      assertFalse(mLockManager.checkLock(wrongSessionId, TEST_BLOCK_ID, lock.get()));
    }
  }

  /**
   * Test when trying to validate a lock of a block via
   * {@link BlockLockManager#checkLock(long, long, long)} with an incorrect block ID.
   */
  @Test
  public void validateLockIdWithWrongBlockId() {
    try (BlockLock lock = mLockManager.acquireBlockLock(TEST_SESSION_ID, TEST_BLOCK_ID,
        BlockLockType.READ)) {
      long wrongBlockId = TEST_BLOCK_ID + 1;
      assertFalse(mLockManager.checkLock(TEST_SESSION_ID, wrongBlockId, lock.get()));
    }
  }

  /**
   * Tests when trying to validate a lock of a block via
   * {@link BlockLockManager#checkLock(long, long, long)} after the session was cleaned up.
   */
  @Test
  public void cleanupSession() {
    long sessionId1 = TEST_SESSION_ID;
    long sessionId2 = TEST_SESSION_ID + 1;
    BlockLock lock1 = mLockManager.acquireBlockLock(sessionId1, TEST_BLOCK_ID, BlockLockType.READ);
    BlockLock lock2 = mLockManager.acquireBlockLock(sessionId2, TEST_BLOCK_ID, BlockLockType.READ);
    mLockManager.cleanupSession(sessionId2);
    // Expect validating sessionId1 to get through
    assertTrue(mLockManager.checkLock(sessionId1, TEST_BLOCK_ID, lock1.get()));
    // Because sessionId2 has been cleaned up, expect validating sessionId2 to return false
    assertFalse(mLockManager.checkLock(sessionId2, TEST_BLOCK_ID, lock2.get()));
  }

  /**
   * Tests that up to WORKER_TIERED_STORE_BLOCK_LOCKS block locks can be grabbed simultaneously.
   */
  @Test(timeout = 10000)
  public void grabManyLocks() throws Exception {
    int maxLocks = 100;
    setMaxLocks(maxLocks);
    BlockLockManager manager = new BlockLockManager();
    for (int i = 0; i < maxLocks; i++) {
      manager.acquireBlockLock(i, i, BlockLockType.WRITE);
    }
    lockExpectingHang(manager, 101);
  }

  /**
   * Tests that an exception is thrown when a session tries to acquire a write lock on a block that
   * it currently has a read lock on.
   */
  @Test
  public void lockAlreadyReadLockedBlock() {
    BlockLockManager manager = new BlockLockManager();
    try (BlockLock readLock = manager.acquireBlockLock(1, 1, BlockLockType.READ)) {
      mThrown.expect(IllegalStateException.class);
      manager.acquireBlockLock(1, 1, BlockLockType.WRITE);
    }
  }

  /**
   * Tests that an exception is thrown when a session tries to acquire a write lock on a block that
   * it currently has a write lock on.
   */
  @Test
  public void lockAlreadyWriteLockedBlock() {
    BlockLockManager manager = new BlockLockManager();
    try (BlockLock readLock = manager.acquireBlockLock(1, 1, BlockLockType.WRITE)) {
      mThrown.expect(IllegalStateException.class);
      manager.acquireBlockLock(1, 1, BlockLockType.WRITE);
    }
  }

  /**
   * Tests that two sessions can both take a read lock on the same block.
   */
  @Test(timeout = 10000)
  public void lockAcrossSessions() {
    BlockLockManager manager = new BlockLockManager();
    try (BlockLock lock1 = manager.acquireBlockLock(1, TEST_BLOCK_ID, BlockLockType.READ);
         BlockLock lock2 = manager.acquireBlockLock(2, TEST_BLOCK_ID, BlockLockType.READ)) {
      //do nothing
    }
  }

  /**
   * Tests that a write lock can't be taken while a read lock is held.
   */
  @Test(timeout = 10000)
  public void readBlocksWrite() throws Exception {
    BlockLockManager manager = new BlockLockManager();
    try (BlockLock lock = manager.acquireBlockLock(1, TEST_BLOCK_ID, BlockLockType.READ)) {
      lockExpectingHang(manager, TEST_BLOCK_ID);
    }
  }

  /**
   * Tests that block locks are returned to the pool when they are no longer in use.
   */
  @Test(timeout = 10000)
  public void reuseLock() {
    setMaxLocks(1);
    BlockLockManager manager = new BlockLockManager();
    BlockLock lock1 = manager.acquireBlockLock(TEST_SESSION_ID, 1, BlockLockType.WRITE);
    lock1.close(); // Without this line the next lock would hang.
    manager.acquireBlockLock(TEST_SESSION_ID, 2, BlockLockType.WRITE);
  }

  /**
   * Tests that block locks are not returned to the pool when they are still in use.
   */
  @Test(timeout = 10000)
  public void dontReuseLock() throws Exception {
    setMaxLocks(1);
    final BlockLockManager manager = new BlockLockManager();
    try (BlockLock lock = manager.acquireBlockLock(TEST_SESSION_ID, 1, BlockLockType.READ)) {
      manager.acquireBlockLock(TEST_SESSION_ID, 1, BlockLockType.READ);
    }
    lockExpectingHang(manager, 2);
  }

  /**
   * Calls {@link BlockLockManager#acquireBlockLock(long, long, BlockLockType)}
   * and fails if it doesn't hang.
   *
   * @param manager the manager to call lock on
   * @param blockId block id to try locking
   */
  private void lockExpectingHang(final BlockLockManager manager, final long blockId)
      throws Exception {
    Thread thread = new Thread(
        () -> manager.acquireBlockLock(TEST_SESSION_ID, blockId, BlockLockType.WRITE));
    thread.start();
    thread.join(200);
    // Locking should not take 200ms unless there is a hang.
    assertTrue(thread.isAlive());
  }

  /**
   * Tests that taking and releasing many block locks concurrently won't cause a failure.
   *
   * This is done by creating 200 threads, 100 for each of 2 different block ids. Each thread locks
   * and then unlocks its block 50 times. After this, it takes a final lock on its block before
   * returning. At the end of the test, the internal state of the lock manager is validated.
   */
  @Test(timeout = 10000)
  public void stress() throws Throwable {
    final int numBlocks = 2;
    final int threadsPerBlock = 100;
    final int lockUnlocksPerThread = 50;
    setMaxLocks(numBlocks);
    final BlockLockManager manager = new BlockLockManager();
    final List<Thread> threads = new ArrayList<>();
    final CyclicBarrier barrier = new CyclicBarrier(numBlocks * threadsPerBlock);
    // If there are exceptions, we will store them here.
    final ConcurrentHashSet<Throwable> failedThreadThrowables = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = (th, ex) -> failedThreadThrowables.add(ex);
    for (int blockId = 0; blockId < numBlocks; blockId++) {
      final int finalBlockId = blockId;
      for (int i = 0; i < threadsPerBlock; i++) {
        Thread t = new Thread(() -> {
          try {
            barrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          // Lock and unlock the block lockUnlocksPerThread times.
          for (int j = 0; j < lockUnlocksPerThread; j++) {
            BlockLock lock =
                manager.acquireBlockLock(TEST_SESSION_ID, finalBlockId, BlockLockType.READ);
            lock.close();
          }
          // Lock the block one last time.
          manager.acquireBlockLock(TEST_SESSION_ID, finalBlockId, BlockLockType.READ);
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
    if (!failedThreadThrowables.isEmpty()) {
      StringBuilder sb = new StringBuilder("Failed with the following errors:\n");
      for (Throwable failedThreadThrowable : failedThreadThrowables) {
        sb.append(Throwables.getStackTraceAsString(failedThreadThrowable));
      }
      Assert.fail(sb.toString());
    }
    manager.validate();
  }

  private void setMaxLocks(int maxLocks) {
    Configuration.set(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS,
        maxLocks);
  }
}
