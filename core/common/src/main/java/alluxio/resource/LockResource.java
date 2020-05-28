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

package alluxio.resource;

import alluxio.exception.ExceptionMessage;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

/**
 * A resource lock that makes it possible to acquire and release locks using the following idiom:
 *
 * <pre>
 *   try (LockResource r = new LockResource(lock)) {
 *     ...
 *   }
 * </pre>
 */
// extends Closeable instead of AutoCloseable to enable usage with Guava's Closer.
public class LockResource implements Closeable {
  private final Lock mLock;

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   */
  public LockResource(Lock lock) {
    this(lock, true);
  }

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * @param lock the lock to acquire
   * @param acquireLock whether to lock the lock
   */
  public LockResource(Lock lock, boolean acquireLock) {
    mLock = lock;
    if (acquireLock) {
      mLock.lock();
    }
  }

  /**
   * Creates a new instance of {@link LockResource} using the given lock.
   *
   * This method will use the {@link Lock#tryLock(long, TimeUnit)} internally in a loop
   * for trying to grab the lock without causing total blockage of other waiters of the lock.
   *
   * @param lock  the lock to acquire
   * @param tryMs the duration to attempt acquiring the lock
   * @param sleepMs the wait duration after failed attempt to acquire the lock
   * @param timeoutMs the total duration to try acquiring the lock in a loop
   * @throws InterruptedException if interrupted while acquiring the lock
   * @throws TimeoutException if the lock was not acquired after given timeout
   */
  public LockResource(Lock lock, long tryMs, long sleepMs, long timeoutMs)
      throws InterruptedException, TimeoutException {
    mLock = lock;
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    boolean lockAcquired = false;
    while (System.currentTimeMillis() < deadlineMs) {
      if (mLock.tryLock(tryMs, TimeUnit.MILLISECONDS)) {
        lockAcquired = true;
        break;
      } else {
        long remainingWaitMs = deadlineMs - System.currentTimeMillis();
        if (remainingWaitMs > 0) {
          Thread.sleep(Math.min(sleepMs, remainingWaitMs));
        }
      }
    }
    if (!lockAcquired) {
      throw new TimeoutException(ExceptionMessage.STATE_LOCK_TIMED_OUT.getMessage(timeoutMs));
    }
  }

  /**
   * Returns true if the other {@link LockResource} contains the same lock.
   *
   * @param other other LockResource
   * @return true if the other lockResource has the same lock
   */
  @VisibleForTesting
  public boolean hasSameLock(LockResource other) {
    return mLock == other.mLock;
  }

  /**
   * Releases the lock.
   */
  @Override
  public void close() {
    mLock.unlock();
  }
}
