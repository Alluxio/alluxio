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

import alluxio.Constants;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.resource.LockResource;
import alluxio.retry.RetryUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;
import alluxio.util.logging.SamplingLogger;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Provides graceful and interruptable locking protocol for taking the state lock.
 *
 * {@link #lockShared()} will be used by user RPCs and may throw {@link InterruptedException}
 * based on options passed to {@link #lockExclusive(StateLockOptions)}.
 *
 * {@link #lockExclusive(StateLockOptions)} will be used by metadata backups in order to
 * guarantee paused state during critical tasks.
 */
public class StateLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(StateLockManager.class);
  private static final SamplingLogger SAMPLING_LOG =
      new SamplingLogger(LOG, 30 * Constants.SECOND_MS);
  private static final int READ_LOCK_COUNT_HIGH = 20000;

  /** The state-lock. */
  private ReentrantReadWriteLock mStateLock = new ReentrantReadWriteLock(true);

  /** The set of threads that are waiting for or holding the state-lock in shared mode. */
  private Set<Thread> mSharedWaitersAndHolders;
  /** Scheduler that is used for interrupt-cycle. */
  private ScheduledExecutorService mScheduler;

  /** Whether exclusive locking will trigger interrupt-cycle. */
  private boolean mInterruptCycleEnabled;
  /** Interval at which threads around shared-lock will be interrupted during interrupt-cycle. */
  private long mInterruptCycleInterval;
  /** Used to synchronize execution/termination of interrupt-cycle. */
  private Lock mInterruptCycleLock = new ReentrantLock(true);
  /** How many active exclusive locking attempts. */
  private volatile int mInterruptCycleRefCount = 0;
  /** The future for the active interrupt cycle. */
  private ScheduledFuture<?> mInterrupterFuture;
  /** Whether interrupt-cycle is entered. */
  private AtomicBoolean mInterruptCycleTicking = new AtomicBoolean(false);

  /** This is the deadline for forcing the lock. */
  private long mForcedDurationMs;

  // TODO(ggezer): Make it bound to a process start/stop cycle.
  /** Shared locking requests will fail until this time. */
  private long mExclusiveOnlyDeadlineMs = -1;

  /**
   * Creates a new state-lock manager.
   */
  public StateLockManager() {
    mSharedWaitersAndHolders = new ConcurrentHashSet<>();
    // Init members.
    mScheduler = Executors
        .newSingleThreadScheduledExecutor(ThreadFactoryUtils.build("state-lock-manager-%d", true));
    // Read properties.
    mInterruptCycleEnabled = ServerConfiguration
        .getBoolean(PropertyKey.MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_ENABLED);
    mInterruptCycleInterval =
        ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_STATE_LOCK_INTERRUPT_CYCLE_INTERVAL);
    mForcedDurationMs =
        ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_STATE_LOCK_FORCED_DURATION);
    // Validate properties.
    Preconditions.checkArgument(mInterruptCycleInterval > 0,
        "Interrupt-cycle interval should be greater than 0.");
  }

  /**
   * This is called by owning process in order to signal that
   * the state is read completely and masters are started.
   *
   * This triggers the beginning of exclusive-only maintenance mode for the state-lock.
   * Note: Calling it multiple times does not reset the maintenance window.
   */
  public void mastersStartedCallback() {
    if (mExclusiveOnlyDeadlineMs == -1) {
      long exclusiveOnlyDurationMs =
          ServerConfiguration.getMs(PropertyKey.MASTER_BACKUP_STATE_LOCK_EXCLUSIVE_DURATION);
      mExclusiveOnlyDeadlineMs = System.currentTimeMillis() + exclusiveOnlyDurationMs;
      if (exclusiveOnlyDurationMs > 0) {
        LOG.info("State-lock will remain in exclusive-only mode for {}ms until {}",
            exclusiveOnlyDurationMs, new Date(mExclusiveOnlyDeadlineMs).toString());
      }
    }
  }

  /**
   * Locks the state shared.
   *
   * Calling thread might be interrupted by this manager,
   * if it found to be waiting for the shared lock under when:
   *  - backup is exiting grace-cycle and entering the lock permanently
   *  - backup is in progress
   *
   * @return the lock resource
   * @throws InterruptedException
   */
  public LockResource lockShared() throws InterruptedException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Thread-{} entered lockShared().", ThreadUtils.getCurrentThreadIdentifier());
    }
    if (LOG.isInfoEnabled()) {
      final int readLockCount = mStateLock.getReadLockCount();
      if (readLockCount > READ_LOCK_COUNT_HIGH) {
        SAMPLING_LOG.info("Read Lock Count Too High: {} {}", readLockCount,
            mSharedWaitersAndHolders);
      }
    }

    // Do not allow taking shared lock during safe-mode.
    long exclusiveOnlyRemainingMs = mExclusiveOnlyDeadlineMs - System.currentTimeMillis();
    if (exclusiveOnlyRemainingMs > 0) {
      String safeModeMsg = String.format(
          "Master still in exclusive-only phase (%dms remaining) for the state-lock. "
              + "Please see documentation for %s.",
          exclusiveOnlyRemainingMs, PropertyKey.Name.MASTER_BACKUP_STATE_LOCK_EXCLUSIVE_DURATION);
      throw new IllegalStateException(safeModeMsg);
    }
    // Register thread for interrupt cycle.
    mSharedWaitersAndHolders.add(Thread.currentThread());
    // Grab the lock interruptibly.
    mStateLock.readLock().lockInterruptibly();
    // Return the resource.
    // Register an action to remove the thread from holders registry before releasing the lock.
    return new LockResource(mStateLock.readLock(), false, false, () -> {
      mSharedWaitersAndHolders.remove(Thread.currentThread());
    });
  }

  /**
   * Locks the state exclusively.
   *
   * @param lockOptions exclusive lock options
   * @return the lock resource
   * @throws TimeoutException if locking times out
   * @throws InterruptedException if interrupting during locking
   */
  public LockResource lockExclusive(StateLockOptions lockOptions)
      throws TimeoutException, InterruptedException, IOException {
    return lockExclusive(lockOptions, null);
  }

  /**
   * Locks the state exclusively.
   *
   * @param lockOptions exclusive lock options
   * @param beforeAttempt a function which runs before each lock attempt and returns whether the
   *                      lock should continue
   * @return the lock resource
   * @throws TimeoutException if locking times out
   * @throws InterruptedException if interrupting during locking
   * @throws IOException if the beforeAttempt functions fails
   */
  public LockResource lockExclusive(StateLockOptions lockOptions,
      RetryUtils.RunnableThrowsIOException beforeAttempt)
      throws TimeoutException, InterruptedException, IOException {
    LOG.debug("Thread-{} entered lockExclusive().", ThreadUtils.getCurrentThreadIdentifier());
    // Run the grace cycle.
    StateLockOptions.GraceMode graceMode = lockOptions.getGraceMode();
    boolean graceCycleEntered = false;
    boolean lockAcquired = false;
    long deadlineMs = System.currentTimeMillis() + lockOptions.getGraceCycleTimeoutMs();
    while (System.currentTimeMillis() < deadlineMs) {
      if (!graceCycleEntered) {
        graceCycleEntered = true;
        LOG.info("Thread-{} entered grace-cycle of try-sleep: {}ms-{}ms for the total of {}ms",
            ThreadUtils.getCurrentThreadIdentifier(), lockOptions.getGraceCycleTryMs(),
            lockOptions.getGraceCycleSleepMs(), lockOptions.getGraceCycleTimeoutMs());
      }
      if (beforeAttempt != null) {
        beforeAttempt.run();
      }
      if (mStateLock.writeLock().tryLock(lockOptions.getGraceCycleTryMs(), TimeUnit.MILLISECONDS)) {
        lockAcquired = true;
        break;
      } else {
        long remainingWaitMs = deadlineMs - System.currentTimeMillis();
        if (remainingWaitMs > 0) {
          Thread.sleep(Math.min(lockOptions.getGraceCycleSleepMs(), remainingWaitMs));
        }
      }
    }
    if (lockAcquired) { // Lock was acquired within grace-cycle.
      LOG.info("Thread-{} acquired the lock within grace-cycle.",
          ThreadUtils.getCurrentThreadIdentifier());
      activateInterruptCycle();
    } else { // Lock couldn't be acquired by grace-cycle.
      if (graceMode == StateLockOptions.GraceMode.TIMEOUT) {
        throw new TimeoutException(
            ExceptionMessage.STATE_LOCK_TIMED_OUT.getMessage(lockOptions.getGraceCycleTimeoutMs()));
      }
      // Activate the interrupt cycle before entering the lock because it might wait in the queue.
      activateInterruptCycle();
      // Force the lock.
      LOG.info("Thread-{} forcing the lock with {} waiters/holders: {}",
          ThreadUtils.getCurrentThreadIdentifier(), mSharedWaitersAndHolders.size(),
          mSharedWaitersAndHolders.stream().map((th) -> Long.toString(th.getId()))
              .collect(Collectors.joining(",")));
      try {
        if (beforeAttempt != null) {
          beforeAttempt.run();
        }
        if (!mStateLock.writeLock().tryLock(mForcedDurationMs, TimeUnit.MILLISECONDS)) {
          throw new TimeoutException(ExceptionMessage.STATE_LOCK_TIMED_OUT
              .getMessage(lockOptions.getGraceCycleTimeoutMs() + mForcedDurationMs));
        }
      } catch (Throwable throwable) {
        // Deactivate interrupter if lock acquisition was not successful.
        deactivateInterruptCycle();
        throw throwable;
      }
    }

    // We have the lock, wrap it and return.
    // Register an action for cancelling the interrupt cycle before releasing the lock.
    return new LockResource(mStateLock.writeLock(), false, false, () -> {
      // Before releasing the write-lock, activate interrupter if active.
      deactivateInterruptCycle();
    });
  }

  /**
   * @return the list of thread identifiers that are waiting and holding on the shared lock
   */
  public List<String> getSharedWaitersAndHolders() {
    List<String> result = new ArrayList<>();

    for (Thread waiterOrHolder : mSharedWaitersAndHolders) {
      result.add(ThreadUtils.getThreadIdentifier(waiterOrHolder));
    }
    return result;
  }

  /**
   * @return {@code true} if the interrupt-cycle has been ticked and ticking
   */
  public boolean interruptCycleTicking() {
    return mInterruptCycleTicking.get();
  }

  /**
   * Schedules the cycle of interrupting state-lock waiters/holders.
   * It's called when:
   *  - Lock is acquired by grace-cycle
   *  - Lock is being taken directly after unsuccessful grace-cycle
   *
   * Calling it multiple times only schedules one interrupt-cycle.
   */
  private void activateInterruptCycle() {
    if (!mInterruptCycleEnabled) {
      return;
    }
    try (LockResource lr = new LockResource(mInterruptCycleLock)) {
      // Don't reschedule if it was before.
      if (mInterruptCycleRefCount++ > 0) {
        return;
      }
      // Setup the cycle.
      LOG.info("Interrupt cycle activated.");
      mInterrupterFuture = mScheduler.scheduleAtFixedRate(this::waiterInterruptRoutine,
          mInterruptCycleInterval, mInterruptCycleInterval, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Stops the cycle of interrupting state-lock waiters/holders.
   *
   * Interrupt-cycle will be stopped when this method is called as much as
   * {@link #activateInterruptCycle()}.
   */
  private void deactivateInterruptCycle() {
    if (!mInterruptCycleEnabled) {
      return;
    }
    try (LockResource lr = new LockResource(mInterruptCycleLock)) {
      Preconditions.checkArgument(mInterruptCycleRefCount > 0);
      // Don't do anything if there are exclusive lockers.
      if (--mInterruptCycleRefCount > 0) {
        return;
      }
      // Cancel the cycle.
      mInterrupterFuture.cancel(true);
      mInterruptCycleTicking.set(false);
      LOG.info("Interrupt cycle deactivated.");
      mInterrupterFuture = null;
    }
  }

  /**
   * Scheduled routine that interrupts waiters/holders of shared lock.
   */
  private void waiterInterruptRoutine() {
    mInterruptCycleTicking.set(true);
    // Keeping a list of interrupted threads for logging consistently at the end.
    List<Thread> interruptedThreads = new ArrayList(mSharedWaitersAndHolders.size());
    // Interrupt threads that are registered under shared lock.
    for (Thread th : mSharedWaitersAndHolders) {
      th.interrupt();
      interruptedThreads.add(th);
    }
    LOG.info("Interrupt-cycle interrupted {} waiters/holders: {}", interruptedThreads.size(),
        interruptedThreads.stream().map((th) -> ThreadUtils.getThreadIdentifier(th))
            .collect(Collectors.joining(",")));
  }
}
