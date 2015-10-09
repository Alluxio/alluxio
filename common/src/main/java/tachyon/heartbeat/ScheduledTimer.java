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

package tachyon.heartbeat;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class can be used for controlling the time when a heartbeat executes.
 */
public final class ScheduledTimer implements HeartbeatTimer {
  private final String mThreadName;
  private final Lock mLock;
  private final Condition mCondition;

  /**
   * Creates a new instance of {@link ScheduledTimer}
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval (unused)
   */
  public ScheduledTimer(String threadName, long intervalMs) {
    mThreadName = threadName;
    mLock = new ReentrantLock();
    mCondition = mLock.newCondition();
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
  public void schedule() {
    mLock.lock();
    mCondition.signal();
    mLock.unlock();
  }

  /**
   * Waits until the heartbeat is scheduled for execution.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public synchronized void tick() throws InterruptedException {
    HeartbeatScheduler.addTimer(this);
    mLock.lock();
    mCondition.await();
    mLock.unlock();
  }
}
