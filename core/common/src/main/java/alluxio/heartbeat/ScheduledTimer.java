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

package alluxio.heartbeat;

import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class can be used for controlling heartbeat execution of threads.
 *
 * In particular, the {@link ScheduledTimer} blocks on the {@link #tick()} method, waiting for
 * someone to invoke the {@link #schedule()} method.
 *
 * The contract of this class is that the {@link #schedule()} method should only be called after the
 * {@link #tick()} was called and that there is exactly one {@link #schedule()} call per
 * {@link #tick()} call.
 *
 * The {@link #schedule()} method is not meant to be invoked directly. Instead, the
 * {@link HeartbeatScheduler} class should be used.
 */
@ThreadSafe
public final class ScheduledTimer implements HeartbeatTimer {
  private final String mThreadName;
  private final Lock mLock;
  /** This condition is signaled to tell the heartbeat thread to do a run. */
  private final Condition mTickCondition;
  /** True when schedule() has been called, but tick() hasn't finished. **/
  private volatile boolean mScheduled;

  /**
   * Creates a new instance of {@link ScheduledTimer}.
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval (unused)
   */
  public ScheduledTimer(String threadName, long intervalMs) {
    mThreadName = threadName;
    mLock = new ReentrantLock();
    mTickCondition = mLock.newCondition();
    mScheduled = false;
    // There should never be more than one scheduled timer with the same name.
    HeartbeatScheduler.clearTimer(mThreadName);
  }

  /**
   * @return the thread name
   */
  public String getThreadName() {
    return mThreadName;
  }

  /**
   * Schedules execution of the heartbeat.
   */
  protected void schedule() {
    try (LockResource r = new LockResource(mLock)) {
      Preconditions.checkState(!mScheduled, "Called schedule twice without waiting for any ticks");
      mScheduled = true;
      mTickCondition.signal();
      HeartbeatScheduler.removeTimer(this);
    }
  }

  /**
   * Waits until the heartbeat is scheduled for execution.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public void tick() throws InterruptedException {
    try (LockResource r = new LockResource(mLock)) {
      HeartbeatScheduler.addTimer(this);
      // Wait in a loop to handle spurious wakeups
      while (!mScheduled) {
        mTickCondition.await();
      }

      mScheduled = false;
    }
  }
}
