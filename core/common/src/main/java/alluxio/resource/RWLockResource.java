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

import alluxio.concurrent.LockMode;

import com.google.common.base.Preconditions;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The {@link RWLockResource} is an extension of the {@link LockResource} which allows for
 * downgrading of locks.
 */
public class RWLockResource extends LockResource {

  private final ReentrantReadWriteLock mRwLock;

  /**
   * Creates a new instance of RW lock that will lock with the given mode.
   *
   * @param rwLock the read-write lock backing the resource
   * @param mode the initial lock mode if acquiring the lock
   * @param acquireLock whether or not to acquire the lock
   * @param useTryLock whether or not to use {@link java.util.concurrent.locks.Lock#tryLock} when
   *                  acquiring the resource
   */
  public RWLockResource(ReentrantReadWriteLock rwLock, LockMode mode, boolean acquireLock,
      boolean useTryLock) {
    super(mode == LockMode.READ ? rwLock.readLock() : rwLock.writeLock(), acquireLock, useTryLock);
    mRwLock = rwLock;
  }

  /**
   * Downgrade from a write to a read lock.
   *
   * @return if a successful downgrade was performed. Returns false if it was read locked
   */
  public boolean downgrade() {
    if (!mRwLock.isWriteLocked()) {
      return false;
    }
    Preconditions.checkState(mRwLock.isWriteLockedByCurrentThread(),
        "Lock downgrades may only be initiated by the holding thread.");
    Preconditions.checkState(mLock == mRwLock.writeLock(), "mLock must be the same as mRwLock");

    // Downgrade by taking the read lock and then unlocking the write lock.
    mRwLock.readLock().lock();
    mLock.unlock();
    mLock = mRwLock.readLock();
    return true;
  }
}
