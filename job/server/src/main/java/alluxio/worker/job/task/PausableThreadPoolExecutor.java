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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ThreadPoolExecutor that can be temporarily paused to prevent any threads from
 * executing new tasks.
 */
public class PausableThreadPoolExecutor extends ThreadPoolExecutor {

  // write/read locked by mPauseLock
  private boolean mIsPaused;

  // writes are locked by mPauseLock
  private int mNumPaused;

  private ReentrantLock mPauseLock;
  private Condition mUnpaused;

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
  public PausableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    mIsPaused = false;
    mNumPaused = 0;
    mPauseLock =  new ReentrantLock();
    mUnpaused = mPauseLock.newCondition();
  }

  /**
   * @return number of active threads subtracted by the number of tasks paused
   */
  public int getNumActiveTasks() {
    // the read for mNumPaused is not locked so the value might be off by 1
    // but that is within the expected range.
    mPauseLock.lock();
    try {
      return super.getActiveCount() - mNumPaused;
    } finally {
      mPauseLock.unlock();
    }
  }

  /**
   * Pause all threads from executing new tasks.
   */
  public void pause() {
    mPauseLock.lock();
    try {
      mIsPaused = true;
    } finally {
      mPauseLock.unlock();
    }
  }

  /**
   * Resume all threads to run new tasks.
   */
  public void resume() {
    mPauseLock.lock();
    try {
      if (mIsPaused) {
        mIsPaused = false;
        mUnpaused.signalAll();
      }
    } finally {
      mPauseLock.unlock();
    }
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    mPauseLock.lock();
    try {
      mNumPaused++;
      while (mIsPaused) {
        mUnpaused.await();
      }
    } catch (InterruptedException e) {
      t.interrupt();
    } finally {
      mNumPaused--;
      mPauseLock.unlock();
    }
  }
}
