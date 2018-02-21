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

import alluxio.clock.SystemClock;
import alluxio.time.Sleeper;
import alluxio.time.ThreadSleeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

import javax.annotation.concurrent.NotThreadSafe;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class can be used for executing heartbeats periodically.
 */
@NotThreadSafe
public final class SleepingTimer implements HeartbeatTimer {
  private final long mIntervalMs;
  private long mPreviousTickMs;
  private final String mThreadName;
  private final Logger mLogger;
  private final Clock mClock;
  private final Sleeper mSleeper;

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval
   */
  public SleepingTimer(String threadName, long intervalMs) {
    this(threadName, intervalMs, LoggerFactory.getLogger(SleepingTimer.class),
        new SystemClock(), ThreadSleeper.INSTANCE);
  }

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval
   * @param logger the logger to log to
   * @param clock for telling the current time
   * @param sleeper the utility to use for sleeping
   */
  public SleepingTimer(String threadName, long intervalMs, Logger logger, Clock clock,
      Sleeper sleeper) {
    mIntervalMs = intervalMs;
    mThreadName = threadName;
    mLogger = logger;
    mClock = clock;
    mSleeper = sleeper;
  }

  /**
   * Enforces the thread waits for the given interval between consecutive ticks.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public void tick() throws InterruptedException {
    if (mPreviousTickMs != 0) {
      long executionTimeMs = mClock.millis() - mPreviousTickMs;
      if (executionTimeMs > mIntervalMs) {
        mLogger.warn("{} last execution took {} ms. Longer than the interval {}", mThreadName,
            executionTimeMs, mIntervalMs);
      } else {
        mSleeper.sleep(Duration.ofMillis(mIntervalMs - executionTimeMs));
      }
    }
    mPreviousTickMs = mClock.millis();
  }
}
