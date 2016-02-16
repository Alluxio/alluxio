/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Read/write lock associated with clients rather than threads. Either its read lock or write lock
 * can be released by a thread different from the one acquiring them (but supposed to be requested
 * by the same client).
 */
@ThreadSafe
public final class ClientRWLock implements ReadWriteLock {
  // TODO(bin): Make this const a configurable.
  /** Total number of permits. This value decides the max number of concurrent readers */
  private static final int MAX_AVAILABLE = 100;
  /** Underlying Semaphore */
  private final Semaphore mAvailable = new Semaphore(MAX_AVAILABLE, true);

  @Override
  public Lock readLock() {
    return new SessionLock(1);
  }

  @Override
  public Lock writeLock() {
    return new SessionLock(MAX_AVAILABLE);
  }

  private final class SessionLock implements Lock {
    private final int mPermits;

    private SessionLock(int permits) {
      mPermits = permits;
    }

    @Override
    public void lock() {
      mAvailable.acquireUninterruptibly(mPermits);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      mAvailable.acquire(mPermits);
    }

    @Override
    public boolean tryLock() {
      return mAvailable.tryAcquire(mPermits);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      try {
        return mAvailable.tryAcquire(mPermits, time, unit);
      } catch (InterruptedException e) {
        return false;
      }
    }

    @Override
    public void unlock() {
      mAvailable.release(mPermits);
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException("newCondition() is not supported");
    }
  }
}
