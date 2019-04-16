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

package alluxio.time;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sleeper which can be manually controlled by tests. This allows time-related classes to be tested
 * deterministically, without relying on polling or calls to Thread.sleep.
 *
 * To use the class, pass an instance of ManualSleeper into the class to be tested, then use the
 * {@link #waitForSleep()} method to verify that the class is sleeping for the right amount of time.
 * After calling {@link #waitForSleep()}, use {@link #wakeUp()} to end the test class's call to
 * {@link #sleep(Duration)}.
 *
 * See {@link ManualSleeperTest} for example usage.
 */
public class ManualSleeper implements Sleeper {
  private final Lock mSleepLock = new ReentrantLock();
  private final Condition mSleepLockCond = mSleepLock.newCondition();

  private Duration mLastSleep = Duration.ZERO;
  private boolean mSleeping = false;

  @Override
  public void sleep(Duration duration) throws InterruptedException {
    mSleepLock.lock();
    mLastSleep = duration;
    mSleeping = true;
    mSleepLockCond.signalAll();
    try {
      while (mSleeping) {
        mSleepLockCond.await();
      }
    } finally {
      mSleeping = false; // handles the case where await() is interrupted
      mSleepLock.unlock();
    }
  }

  /**
   * @return whether the sleeper is currently sleeping
   */
  public boolean sleeping() {
    mSleepLock.lock();
    try {
      return mSleeping;
    } finally {
      mSleepLock.unlock();
    }
  }

  /**
   * Waits for the sleeper to be in a sleeping state and returns the length of time it is sleeping
   * for.
   */
  public Duration waitForSleep() throws InterruptedException {
    mSleepLock.lock();
    try {
      while (!mSleeping) {
        mSleepLockCond.await();
      }
      return mLastSleep;
    } finally {
      mSleepLock.unlock();
    }
  }

  /**
   * Wakes up from the current call to sleep.
   */
  public void wakeUp() {
    mSleepLock.lock();
    Preconditions.checkState(mSleeping, "Called wakeUp when nothing was sleeping");
    try {
      mSleeping = false;
      mSleepLockCond.signal();
    } finally {
      mSleepLock.unlock();
    }
  }
}
