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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class can be used for controlling heartbeat execution.
 */
public final class HeartbeatScheduler {
  private static Map<String, ScheduledTimer> sTimers = new HashMap<String, ScheduledTimer>();
  private static Lock sLock =  new ReentrantLock();
  private static Condition sCondition = sLock.newCondition();

  /**
   * @param timer a timer to add to the scheduler
   */
  public static void addTimer(ScheduledTimer timer) {
    sLock.lock();
    sTimers.put(timer.getThreadName(), timer);
    sCondition.signal();
    sLock.unlock();
  }

  /**
   * @param timer a timer to remove from the scheduler
   */
  public static synchronized void removeTimer(ScheduledTimer timer) {
    sLock.lock();
    sTimers.remove(timer.getThreadName());
    sLock.unlock();
  }

  /**
   * @return the set of threads present in the scheduler
   */
  public static synchronized Set<String> getThreadNames() {
    Set<String> result;
    sLock.lock();
    result = sTimers.keySet();
    sLock.unlock();
    return result;
  }

  /**
   * Schedules execution of a heartbeat for the given thread.
   *
   * @param threadName the thread for which heartbeat is to be executed
   */
  public static void schedule(String threadName) {
    sLock.lock();
    ScheduledTimer timer = sTimers.get(threadName);
    if (timer == null) {
      sLock.unlock();
      throw new RuntimeException("Timer for thread " + threadName + " not found.");
    }
    timer.schedule();
    sTimers.remove(threadName);
    sLock.unlock();
  }

  /**
   * Waits until the given thread can be executed.
   *
   * @param threadName the thread to wait for
   */
  public static void await(String threadName) throws InterruptedException {
    sLock.lock();
    while (!sTimers.containsKey(threadName)) {
      sCondition.await();
    }
    sLock.unlock();
  }
}
