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

  private ReentrantLock throttleLock = new ReentrantLock();
  private Condition throttleReduced = throttleLock.newCondition();

  public ThrottleableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    mHashFunction = Hashing.murmur3_32();
    mThrottlePercentage = 0;
  }

  public void throttle(int throttlePercentage) {
    Preconditions.checkArgument(throttlePercentage >= 0 && throttlePercentage <= 100);
    throttleLock.lock();
    try {
      int prevThrottlePercentage = mThrottlePercentage;
      mThrottlePercentage = throttlePercentage;
      if (prevThrottlePercentage > mThrottlePercentage) {
        throttleReduced.signalAll();
      }
    } finally {
      throttleLock.unlock();
    }
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    throttleLock.lock();
    try {
      while (!check(t)) {
        throttleReduced.await();
      }
    } catch (InterruptedException e) {
      t.interrupt();
    } finally {
      throttleLock.unlock();
    }
  }

  private boolean check(Thread t) {
    return IntMath.mod(mHashFunction.hashLong(t.getId()).asInt(), 100) >= mThrottlePercentage;
  }
}
