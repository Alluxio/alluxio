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

import alluxio.Constants;

import com.google.common.annotations.VisibleForTesting;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * A progressive sleeper that wakes up multiple times during sleep to check if the requested sleep
 * duration has changed, and adjust its sleep duration accordingly.
 * */
public final class SteppingThreadSleeper implements Sleeper {
  private long mSleepStepMs = Constants.MINUTE;

  public static final SteppingThreadSleeper INSTANCE = new SteppingThreadSleeper();

  private final Sleeper mInternalSleeper;
  private final Clock mClock;

  private SteppingThreadSleeper() {
    mInternalSleeper = ThreadSleeper.INSTANCE;
    mClock = Clock.systemUTC();
  }

  /**
   * Creates a new instance of {@link SteppingThreadSleeper}.
   * @param internalSleeper the internal sleeper
   * @param clock for telling the current time
   */
  @VisibleForTesting
  public SteppingThreadSleeper(Sleeper internalSleeper, Clock clock) {
    mInternalSleeper = internalSleeper;
    mClock = clock;
  }

  @Override
  public void sleep(Duration duration) throws InterruptedException {
    mInternalSleeper.sleep(duration);
  }

  @Override
  public void sleep(Supplier<Duration> durationSupplier) throws InterruptedException {
    Duration duration = durationSupplier.get();
    if (duration.toMillis() < 0) {
      return;
    }
    if (duration.toMillis() < mSleepStepMs) {
      sleep(duration);
      return;
    }
    long startSleepMs = mClock.millis();
    long sleepTo = startSleepMs + duration.toMillis();
    long timeNow;
    while ((timeNow = mClock.millis()) < sleepTo) {
      long sleepTime = Math.min(sleepTo - timeNow, mSleepStepMs);
      mInternalSleeper.sleep(Duration.ofMillis(sleepTime));

      long newInterval = durationSupplier.get().toMillis();
      if (newInterval >= 0) {
        sleepTo = startSleepMs + newInterval;
      }
    }
  }

  /**
   * Sets the sleep step.
   *
   * @param sleepStepMs the sleep step
   */
  @VisibleForTesting
  public void setSleepStepMs(long sleepStepMs) {
    mSleepStepMs = sleepStepMs;
  }
}
