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

package alluxio.concurrent;

import alluxio.annotation.SuppressFBWarnings;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ReadWriteLock implementation whose read and write locks retain a reference back to this lock.
 * Otherwise, a reference to just the read lock or just the write lock would not suffice to ensure
 * the {@code ReadWriteLock} is retained.
 *
 * Adapted from
 * https://github.com/google/guava/blob/v27.0.1/guava/src/com/google/common/util/concurrent/Striped.java#L319
 */
public final class WeakSafeReentrantReadWriteLock implements ReadWriteLock {
  private final ReentrantReadWriteLock mDelegate;

  /**
   * Creates a new lock.
   */
  public WeakSafeReentrantReadWriteLock() {
    mDelegate = new ReentrantReadWriteLock();
  }

  @Override
  public Lock readLock() {
    return new WeakSafeLock(mDelegate.readLock(), this);
  }

  @Override
  public Lock writeLock() {
    return new WeakSafeLock(mDelegate.writeLock(), this);
  }

  /**
   * Queries the number of reentrant read holds on this lock by the current thread. A reader thread
   * has a hold on a lock for each lock action that is not matched by an unlock action.
   *
   * @return the number of holds on the read lock by the current thread, or zero if the read lock is
   *         not held by the current thread
   */
  public int getReadHoldCount() {
    return mDelegate.getReadHoldCount();
  }

  /**
   * Queries the number of reentrant write holds on this lock by the current thread. A writer thread
   * has a hold on a lock for each lock action that is not matched by an unlock action.
   *
   * @return the number of holds on the write lock by the current thread, or zero if the write lock
   *         is not held by the current thread
   */
  public int getWriteHoldCount() {
    return mDelegate.getWriteHoldCount();
  }

  /**
   * Lock object that ensures a strong reference is retained to a specified object.
   */
  private static final class WeakSafeLock extends ForwardingLock {
    private final Lock mDelegate;

    @SuppressFBWarnings(value = "URF_UNREAD_FIELD",
        justification = "We just want a reference to prevent gc")
    private final WeakSafeReentrantReadWriteLock mStrongReference;

    WeakSafeLock(Lock delegate, WeakSafeReentrantReadWriteLock strongReference) {
      mDelegate = delegate;
      mStrongReference = strongReference;
    }

    @Override
    Lock delegate() {
      return mDelegate;
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException("newCondition is not currently supported");
    }
  }
}
