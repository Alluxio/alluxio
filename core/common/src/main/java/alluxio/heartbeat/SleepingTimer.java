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

import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class can be used for executing heartbeats periodically.
 */
@NotThreadSafe
public class SleepingTimer implements HeartbeatTimer {
  private long mIntervalMs;
  protected long mPreviousTickMs = -1;
  private final String mThreadName;
  protected final Logger mLogger;
  protected final Clock mClock;
  protected final Sleeper mSleeper;
  protected final Supplier<Long> mIntervalSupplier;

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param clock for telling the current time
   * @param intervalSupplier Sleep time between different heartbeat supplier
   * @param periodCronExpressionSupplier the period cron expression
   */
  public SleepingTimer(String threadName, Clock clock, Supplier<Long> intervalSupplier,
      Supplier<String> periodCronExpressionSupplier) {
    this(threadName, LoggerFactory.getLogger(SleepingTimer.class),
        clock, ThreadSleeper.INSTANCE, intervalSupplier, periodCronExpressionSupplier);
  }

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param logger the logger to log to
   * @param clock for telling the current time
   * @param sleeper the utility to use for sleeping
   * @param intervalSupplier Sleep time between different heartbeat supplier
   * @param periodCronExpressionSupplier the period cron expression (unused)
   */
  public SleepingTimer(String threadName, Logger logger, Clock clock, Sleeper sleeper,
      Supplier<Long> intervalSupplier,
      Supplier<String> periodCronExpressionSupplier) {
    mThreadName = threadName;
    mLogger = logger;
    mClock = clock;
    mSleeper = sleeper;
    mIntervalSupplier = intervalSupplier;
    mIntervalMs = mIntervalSupplier.get();
  }

  /**
   * Enforces the thread waits for the given interval between consecutive ticks.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  @Override
  public long tick() throws InterruptedException {
    if (mPreviousTickMs != -1) {
      long executionTimeMs = mClock.millis() - mPreviousTickMs;
      if (executionTimeMs > mIntervalMs) {
        mLogger.warn("{} last execution took {} ms. Longer than the interval {}", mThreadName,
            executionTimeMs, mIntervalMs);
      } else {
        mSleeper.sleep(Duration.ofMillis(mIntervalMs - executionTimeMs));
      }
    }
    mPreviousTickMs = mClock.millis();
    return Long.MAX_VALUE;
  }

  @Override
  public void update() {
    long interval = mIntervalSupplier.get();
    if (interval != mIntervalMs) {
      mLogger.info("update {} interval from {} to {}", mThreadName, mIntervalMs, interval);
      mIntervalMs = interval;
    }
  }
}
