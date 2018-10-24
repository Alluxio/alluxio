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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

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
  /**
   * A map from thread name to active timer for that thread. A timer is active when its thread is
   * waiting to be scheduled.
   */
  private static Map<String, ScheduledTimer> sTimers = new HashMap<>();
  private static Lock sLock = new ReentrantLock();
  private static Condition sCondition = sLock.newCondition();

  private HeartbeatScheduler() {} // to prevent initialization

  /**
   * @param timer a timer to add to the scheduler
   */
  public static void addTimer(ScheduledTimer timer) {
    Preconditions.checkNotNull(timer, "timer");
    try (LockResource r = new LockResource(sLock)) {
      Preconditions.checkState(!sTimers.containsKey(timer.getThreadName()),
          "The timer for thread %s is already waiting to be scheduled", timer.getThreadName());
      sTimers.put(timer.getThreadName(), timer);
      sCondition.signalAll();
    }
  }

  /**
   * Removes a timer name from the scheduler if it exists.
   *
   * @param name the name to clear
   */
  public static void clearTimer(String name) {
    try (LockResource r = new LockResource(sLock)) {
      sTimers.remove(name);
    }
  }

  /**
   * Removes a timer from the scheduler.
   *
   * This method will fail if the timer is not in the scheduler.
   *
   * @param timer the timer to remove
   */
  public static void removeTimer(ScheduledTimer timer) {
    Preconditions.checkNotNull(timer, "timer");
    try (LockResource r = new LockResource(sLock)) {
      ScheduledTimer removedTimer = sTimers.remove(timer.getThreadName());
      Preconditions.checkNotNull(removedTimer, "sTimers should contain %s", timer.getThreadName());
      Preconditions.checkState(removedTimer == timer,
          "sTimers should contain the timer being removed");
    }
  }

  /**
   * @return the set of threads present in the scheduler
   */
  public static Set<String> getThreadNames() {
    try (LockResource r = new LockResource(sLock)) {
      return sTimers.keySet();
    }
  }

  /**
   * Schedules execution of a heartbeat for the given thread.
   *
   * @param threadName a name of the thread for which heartbeat is to be executed
   */
  public static void schedule(String threadName) {
    try (LockResource r = new LockResource(sLock)) {
      ScheduledTimer timer = sTimers.get(threadName);
      if (timer == null) {
        throw new RuntimeException("Timer for thread " + threadName + " not found.");
      }
      timer.schedule();
    }
  }

  /**
   * Waits for the given thread to be ready to be scheduled.
   *
   * @param name a name of the thread to wait for
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public static void await(String name) throws InterruptedException {
    try (LockResource r = new LockResource(sLock)) {
      while (!sTimers.containsKey(name)) {
        sCondition.await();
      }
    }
  }

  /**
   * Waits until the given thread can be executed, throwing an unchecked exception of the given
   * timeout expires.
   *
   * @param name a name of the thread to wait for
   * @param time the maximum time to wait
   * @param unit the time unit of the {@code time} argument
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public static void await(String name, long time, TimeUnit unit) throws InterruptedException {
    try (LockResource r = new LockResource(sLock)) {
      while (!sTimers.containsKey(name)) {
        if (!sCondition.await(time, unit)) {
          throw new RuntimeException(
              "Timed out waiting for thread " + name + " to be ready for scheduling");
        }
      }
    }
  }

  /**
   * Convenience method for executing a heartbeat and waiting for it to complete.
   *
   * @param name the name of the heartbeat to execute
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public static void execute(String name) throws InterruptedException {
    await(name);
    schedule(name);
    await(name);
  }
}
