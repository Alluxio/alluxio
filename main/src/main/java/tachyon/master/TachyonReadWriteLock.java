/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A simple read-write lock. Right now writes can be starved, but that
 * shouldn't be a big deal for this kind of workload.
 */
public class TachyonReadWriteLock implements ReadWriteLock {

  private AtomicInteger mReaders = new AtomicInteger(0);
  private AtomicBoolean mWriter = new AtomicBoolean(false);

  /**
   * The read lock inside the read-write lock. We don't actually need all the methods provided by
   * the Lock interface, so some of them are dummy methods.
   */
  private class ReadLock implements Lock {
    public void lock() {
      while (mWriter.get()) {
      }
      mReaders.getAndIncrement();
      if (mWriter.get()) {
        mReaders.getAndDecrement();
        readLock();
      }
    }

    public void unlock() {
      mReaders.getAndDecrement();
    }

    // The rest of the methods are dummy methods

    public Condition newCondition() {
      return null;
    }

    public boolean tryLock() {
      return false;
    }

    public boolean tryLock(long time, TimeUnit unit) {
      return false;
    }

    public void lockInterruptibly() {
      return;
    }
  }

  /**
   * The write lock inside the read-write lock. We don't actually need all the methods provided by
   * the Lock interface, so some of them are dummy methods.
   */
  private class WriteLock implements Lock {
    public void lock() {
      while (mReaders.get() > 0 || !mWriter.compareAndSet(false, true)) {
      }
      if (mReaders.get() > 0) {
        mWriter.set(false);
        writeLock();
      }
    }

    public void unlock() {
      mWriter.set(false);
    }

    // The rest of the methods are dummy methods

    public Condition newCondition() {
      return null;
    }

    public boolean tryLock() {
      return false;
    }

    public boolean tryLock(long time, TimeUnit unit) {
      return false;
    }

    public void lockInterruptibly() {
      return;
    }
  }

  private Lock mReadLock = new ReadLock();
  private Lock mWriteLock = new WriteLock();

  /**
   * Returns the read lock associated with the class.
   */
  public Lock readLock() {
    return mReadLock;
  }

  /**
   * Returns the write lock associated with the class.
   */
  public Lock writeLock() {
    return mWriteLock;
  }

  // These methods aren't part of the ReadWriteLock interface, but they might come in handy.

  public void upgrade() {
    while (mReaders.get() > 1 || !mWriter.compareAndSet(false, true)) {
    }
    if (mReaders.get() > 1) {
      mWriter.set(false);
      upgrade();
    }
    mReadLock.unlock();
  }

  public void downgrade() {
    mReaders.getAndIncrement();
    mWriteLock.unlock();
  }
}
