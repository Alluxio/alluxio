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

package alluxio.heartbeat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

/**
 * This class can be used for controlling heartbeat execution of a thread.
 *
 * In particular, the {@link #await(String)} method can be used to wait for a thread to start
 * waiting to be scheduled and the {@link #schedule(String)} method can be used to schedule a thread
 * that is waiting to be scheduled.
 *
 * The contract of this class is that the {@link #schedule(String)} method should only be called for
 * threads that are in fact waiting to be scheduled. If this contract is not met, concurrent
 * execution of a thread and the heartbeat scheduler logic to schedule the thread can lead to a
 * deadlock.
 *
 * For an example of how to use the {@link HeartbeatScheduler}, see unit test of
 * {@link HeartbeatThread}.
 */
@ThreadSafe
public final class HeartbeatScheduler {
  private static Map<String, ScheduledTimer> sTimers = new HashMap<String, ScheduledTimer>();
  private static Lock sLock = new ReentrantLock();
  private static Condition sCondition = sLock.newCondition();

  private HeartbeatScheduler() {} // to prevent initialization

  /**
   * @param timer a timer to add to the scheduler
   */
  public static void addTimer(ScheduledTimer timer) {
    Preconditions.checkNotNull(timer);
    sLock.lock();
    try {
      sTimers.put(timer.getThreadName(), timer);
      sCondition.signalAll();
    } finally {
      sLock.unlock();
    }
  }

  /**
   * @param timer a timer to remove from the scheduler
   */
  public static synchronized void removeTimer(ScheduledTimer timer) {
    Preconditions.checkNotNull(timer);
    sLock.lock();
    try {
      sTimers.remove(timer.getThreadName());
    } finally {
      sLock.unlock();
    }
  }

  /**
   * @return the set of threads present in the scheduler
   */
  public static synchronized Set<String> getThreadNames() {
    sLock.lock();
    try {
      return sTimers.keySet();
    } finally {
      sLock.unlock();
    }
  }

  /**
   * Schedules execution of a heartbeat for the given thread.
   *
   * @param threadName a name of the thread for which heartbeat is to be executed
   */
  public static void schedule(String threadName) {
    sLock.lock();
    try {
      ScheduledTimer timer = sTimers.get(threadName);
      if (timer == null) {
        throw new RuntimeException("Timer for thread " + threadName + " not found.");
      }
      timer.schedule();
      sTimers.remove(threadName);
    } finally {
      sLock.unlock();
    }
  }

  /**
   * Waits until the given thread can be executed.
   *
   * @param name a name of the thread to wait for
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public static void await(String name) throws InterruptedException {
    sLock.lock();
    try {
      while (!sTimers.containsKey(name)) {
        sCondition.await();
      }
    } finally {
      sLock.unlock();
    }
  }

  /**
   * Waits until the given thread can be executed or the given timeout expires.
   *
   * @param name a name of the thread to wait for
   * @param time the maximum time to wait
   * @param unit the time unit of the {@code time} argument
   * @return {@code false} if the waiting time detectably elapsed before return from the method,
   *         else {@code true}
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public static boolean await(String name, long time, TimeUnit unit) throws InterruptedException {
    sLock.lock();
    try {
      while (!sTimers.containsKey(name)) {
        if (!sCondition.await(time, unit)) {
          return false;
        }
      }
    } finally {
      sLock.unlock();
    }
    return true;
  }
}
