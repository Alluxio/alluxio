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

import alluxio.time.Sleeper;
import alluxio.time.ThreadSleeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class can be used for executing heartbeats periodically.
 */
@NotThreadSafe
public class SleepingTimer implements HeartbeatTimer {
  protected long mPreviousTickedMs = -1;
  private final String mThreadName;
  protected final Logger mLogger;
  protected final Clock mClock;
  protected final Sleeper mSleeper;
  protected final Supplier<SleepIntervalSupplier> mIntervalSupplierSupplier;
  protected SleepIntervalSupplier mIntervalSupplier;

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param clock for telling the current time
   * @param intervalSupplierSupplier Sleep time between different heartbeat supplier
   */
  public SleepingTimer(String threadName, Clock clock,
      Supplier<SleepIntervalSupplier> intervalSupplierSupplier) {
    this(threadName, LoggerFactory.getLogger(SleepingTimer.class),
        clock, ThreadSleeper.INSTANCE, intervalSupplierSupplier);
  }

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param logger the logger to log to
   * @param clock for telling the current time
   * @param sleeper the utility to use for sleeping
   * @param intervalSupplierSupplier Sleep time between different heartbeat supplier
   */
  public SleepingTimer(String threadName, Logger logger, Clock clock, Sleeper sleeper,
      Supplier<SleepIntervalSupplier> intervalSupplierSupplier) {
    mThreadName = threadName;
    mLogger = logger;
    mClock = clock;
    mSleeper = sleeper;
    mIntervalSupplierSupplier = intervalSupplierSupplier;
    mIntervalSupplier = intervalSupplierSupplier.get();
  }

  /**
   * Enforces the thread waits for the given interval between consecutive ticks.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  @Override
  public long tick() throws InterruptedException {
    long nextInterval = mIntervalSupplier.getNextInterval(mPreviousTickedMs, mClock.millis());
    if (nextInterval > 0) {
      mSleeper.sleep(Duration.ofMillis(nextInterval));
    }
    mPreviousTickedMs = mClock.millis();
    return mIntervalSupplier.getRunLimit(mPreviousTickedMs);
  }

  @Override
  public void update() {
    mIntervalSupplier = mIntervalSupplierSupplier.get();
    mLogger.info("update {} interval supplier.", mThreadName);
  }
}
