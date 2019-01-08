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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Forwarding wrapper around a {@code Lock}.
 *
 * Adapted from
 * https://github.com/google/guava/blob/v27.0.1/guava/src/com/google/common/util/concurrent/ForwardingLock.java
 */
abstract class ForwardingLock implements Lock {
  abstract Lock delegate();

  @Override
  public void lock() {
    delegate().lock();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    delegate().lockInterruptibly();
  }

  @Override
  public boolean tryLock() {
    return delegate().tryLock();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    return delegate().tryLock(time, unit);
  }

  @Override
  public void unlock() {
    delegate().unlock();
  }

  @Override
  public Condition newCondition() {
    return delegate().newCondition();
  }
}
