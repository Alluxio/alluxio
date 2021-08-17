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

package alluxio.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests {@link StateLockManager} functionality.
 */
public class StateLockManagerTest {

  @Rule
  public ExpectedException mExpected = ExpectedException.none();

  private void configureInterruptCycle(boolean enabled) {
    configureInterruptCycle(enabled, 100);
  }

  private void configureInterruptCycle(boolean enabled, long intervalMs) {
    ServerConfiguration.set(PropertyKey.MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_ENABLED, enabled);
    ServerConfiguration.set(PropertyKey.MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_INTERVAL,
        intervalMs);
  }

  @Test
  public void testGraceMode_Timeout() throws Throwable {
    configureInterruptCycle(false);
    // The state-lock instance.
    StateLockManager stateLockManager = new StateLockManager();
    // Start a thread that owns the state-lock in shared mode.
    StateLockingThread sharedHolderThread = new StateLockingThread(stateLockManager, false);
    sharedHolderThread.start();
    sharedHolderThread.waitUntilStateLockAcquired();
    // Expect timeout when the lock is held in shared mode.
    mExpected.expect(TimeoutException.class);
    stateLockManager
        .lockExclusive(new StateLockOptions(StateLockOptions.GraceMode.TIMEOUT, 10, 0, 100));
    // Exit the shared holder.
    sharedHolderThread.unlockExit();
    sharedHolderThread.join();
    // Create an exclusive owner of the state-lock.
    StateLockingThread exclusiveHolderThread = new StateLockingThread(stateLockManager, true);
    exclusiveHolderThread.start();
    exclusiveHolderThread.waitUntilStateLockAcquired();
    // Expect timeout when the lock is held in exclusive mode.
    mExpected.expect(TimeoutException.class);
    stateLockManager
        .lockExclusive(new StateLockOptions(StateLockOptions.GraceMode.TIMEOUT, 10, 0, 100));
    // Exit the exclusive holder.
    exclusiveHolderThread.unlockExit();
    exclusiveHolderThread.join();
    // Now the lock can be acquired within the grace-cycle.
    try (LockResource lr = stateLockManager
        .lockExclusive(new StateLockOptions(StateLockOptions.GraceMode.TIMEOUT, 10, 0, 100))) {
      // Acquired within the grace-cycle with no active holder.
    }
  }

  @Test
  public void testGraceMode_Forced() throws Throwable {
    // Enable interrupt-cycle with 100ms interval.
    configureInterruptCycle(true, 100);
    // The state-lock instance.
    StateLockManager stateLockManager = new StateLockManager();
    // Start a thread that owns the state-lock in shared mode.
    StateLockingThread sharedHolderThread = new StateLockingThread(stateLockManager, false);
    sharedHolderThread.start();
    sharedHolderThread.waitUntilStateLockAcquired();
    // Take the state-lock exclusively with GUARANTEED grace mode.
    try (LockResource lr = stateLockManager
        .lockExclusive(new StateLockOptions(StateLockOptions.GraceMode.FORCED, 10, 0, 100))) {
      // Holder should have been interrupted.
      Assert.assertTrue(sharedHolderThread.lockInterrupted());
      sharedHolderThread.join();
      // Spawn a new thread that waits on the lock.
      StateLockingThread sharedWaiterThread = new StateLockingThread(stateLockManager, false);
      sharedWaiterThread.start();
      // Wait until it's interrupted by the cycle too.
      CommonUtils.waitFor("waiter interrupted", () -> sharedWaiterThread.lockInterrupted());
      sharedWaiterThread.join();
    }
  }

  @Test
  public void testExclusiveOnlyMode() throws Throwable {
    // Configure exclusive-only duration to cover the entire test execution.
    final long exclusiveOnlyDurationMs = 30 * 1000;
    ServerConfiguration.set(PropertyKey.MASTER_BACKUP_STATE_LOCK_EXCLUSIVE_DURATION,
        exclusiveOnlyDurationMs);

    // The state-lock instance.
    StateLockManager stateLockManager = new StateLockManager();
    // Simulate masters-started event to initiate the exclusive-only phase.
    stateLockManager.mastersStartedCallback();

    for (int i = 0; i < 10; i++) {
      StateLockingThread sharedHolderThread = new StateLockingThread(stateLockManager, false);
      sharedHolderThread.start();
      // Shared lockers are expected to fail.
      mExpected.expect(IllegalStateException.class);
      sharedHolderThread.waitUntilStateLockAcquired();
    }

    // Exclusive locking should be allowed.
    StateLockingThread exclusiveHolderThread = new StateLockingThread(stateLockManager, true);
    exclusiveHolderThread.start();
    // State lock should be acquired.
    exclusiveHolderThread.waitUntilStateLockAcquired();
    // Signal exit and wait for the exclusive locker.
    exclusiveHolderThread.unlockExit();
    exclusiveHolderThread.join();
  }

  @Test
  public void testGetStateLockSharedWaitersAndHolders() throws Throwable {
    final StateLockManager stateLockManager = new StateLockManager();

    assertEquals(0, stateLockManager.getSharedWaitersAndHolders().size());

    for (int i = 1; i < 10; i++) {
      StateLockingThread sharedHolderThread = new StateLockingThread(stateLockManager, false);
      sharedHolderThread.start();
      sharedHolderThread.waitUntilStateLockAcquired();
      final List<String> sharedWaitersAndHolders = stateLockManager.getSharedWaitersAndHolders();
      assertEquals(i, sharedWaitersAndHolders.size());
      assertTrue(sharedWaitersAndHolders.contains(
          ThreadUtils.getThreadIdentifier(sharedHolderThread)));
    }
  }

  /**
   * Test thread that:
   *  1- locks on state-lock with requested mode shared/exclusive.
   *  2- fires that state-lock is acquired.
   *  3- sleeps on internal lock before exiting, while holding the state-lock.
   *  4- releases the state-lock right before exiting.
   */
  class StateLockingThread extends Thread {
    private StateLockManager mStateLockManager;
    private boolean mExclusive;
    private Lock mExitLock = new ReentrantLock();
    private SettableFuture<Void> mStateLockAcquired;
    private boolean mInterrupted = false;

    /**
     * Creates a state-locking test thread.
     *
     * @param stateLockManager state lock manager
     * @param exclusive whether to acquire the state-lock exclusives
     */
    public StateLockingThread(StateLockManager stateLockManager, boolean exclusive) {
      mStateLockManager = stateLockManager;
      mExclusive = exclusive;
      mStateLockAcquired = SettableFuture.create();
      mExitLock.lock();
    }

    @Override
    public void run() {
      LockResource lr = null;
      try {
        if (mExclusive) {
          lr = mStateLockManager.lockExclusive(StateLockOptions.defaults());
        } else {
          lr = mStateLockManager.lockShared();
        }
        mStateLockAcquired.set(null);

        mExitLock.lockInterruptibly();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          mInterrupted = true;
        }
        mStateLockAcquired.setException(e);
      } finally {
        if (lr != null) {
          lr.close();
        }
      }
    }

    /**
     * Allows thread to exit after acquiring the state-lock.
     */
    public void unlockExit() {
      mExitLock.unlock();
    }

    /**
     * Holds the caller until this thread acquires the state-lock.
     *
     * @throws Exception that is received while acquiring the state-lock
     */
    public void waitUntilStateLockAcquired() throws Throwable {
      try {
        mStateLockAcquired.get();
      } catch (ExecutionException e) {
        if (e.getCause() != null) {
          throw e.getCause();
        } else {
          throw e;
        }
      }
    }

    /**
     * @return {@code true} if this thread is interrupted around locks
     */
    public boolean lockInterrupted() {
      return mInterrupted;
    }
  }
}
