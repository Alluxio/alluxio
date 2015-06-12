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

package tachyon.worker;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Read/write lock associated with clients rather than threads. Either its read lock or write lock
 * can be released by a thread different from the thread acquiring them.
 */
public class ClientRWLock implements ReadWriteLock {
  private static final int MAX_AVAILABLE = 100;
  private final Semaphore mAvailable = new Semaphore(MAX_AVAILABLE, true);

  @Override
  public Lock readLock() {
    return new UserLock(mAvailable, 1);
  }

  @Override
  public Lock writeLock() {
    return new UserLock(mAvailable, MAX_AVAILABLE);
  }

  private class UserLock implements Lock {
    private final int mPermits;
    private Semaphore mSemaphore;

    private UserLock(Semaphore semaphore, int permits) {
      mSemaphore = semaphore;
      mPermits = permits;
    }

    @Override
    public void lock() {
      mSemaphore.acquireUninterruptibly(mPermits);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      mSemaphore.acquire(mPermits);
    }

    @Override
    public boolean tryLock() {
      return mSemaphore.tryAcquire(mPermits);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      try {
        return mSemaphore.tryAcquire(mPermits, time, unit);
      } catch (InterruptedException e) {
        return false;
      }
    }

    @Override
    public void unlock() {
      mSemaphore.release(mPermits);
    }

    @Override
    public Condition newCondition() {
      // Not supported
      return null;
    }
  }
}
