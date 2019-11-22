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

package alluxio.worker.job.task;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.IntMath;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An extension of ThreadPoolExecutor to handle throttling some threads.
 */
public class ThrottleableThreadPoolExecutor extends ThreadPoolExecutor {

  private int mThrottlePercentage;
  HashFunction mHashFunction;

  private ReentrantLock mThrottleLock = new ReentrantLock();
  private Condition mThrottleReduced = mThrottleLock.newCondition();

  /**
   * Copy of one of the constructors in {@link ThreadPoolExecutor}.
   *
   * @param corePoolSize the core pool size
   * @param maximumPoolSize the maximum pool size
   * @param keepAliveTime the keep alive time
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   */
  public ThrottleableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    mHashFunction = Hashing.murmur3_32();
    mThrottlePercentage = 0;
  }

  /**
   * Sets a percentage of threads to be throttled.
   *
   * @param throttlePercentage throttle percentage (0-100)
   */
  public void throttle(int throttlePercentage) {
    Preconditions.checkArgument(throttlePercentage >= 0 && throttlePercentage <= 100);
    mThrottleLock.lock();
    try {
      int prevThrottlePercentage = mThrottlePercentage;
      mThrottlePercentage = throttlePercentage;
      if (prevThrottlePercentage > mThrottlePercentage) {
        mThrottleReduced.signalAll();
      }
    } finally {
      mThrottleLock.unlock();
    }
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    mThrottleLock.lock();
    try {
      while (!check(t)) {
        mThrottleReduced.await();
      }
    } catch (InterruptedException e) {
      t.interrupt();
    } finally {
      mThrottleLock.unlock();
    }
  }

  private boolean check(Thread t) {
    return IntMath.mod(mHashFunction.hashLong(t.getId()).asInt(), 100) >= mThrottlePercentage;
  }
}
