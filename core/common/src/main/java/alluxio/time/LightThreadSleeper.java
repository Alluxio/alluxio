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
 * A light sleeping utility which delegates to Thread.sleep().
 */
public final class LightThreadSleeper implements Sleeper {
  private static final long LIGHT_SLEEP_INTERVAL_MS = Constants.MINUTE;

  public static final LightThreadSleeper INSTANCE = new LightThreadSleeper();

  private final Sleeper mInternalSleeper;
  private final Clock mClock;

  private LightThreadSleeper() {
    mInternalSleeper = ThreadSleeper.INSTANCE;
    mClock = Clock.systemUTC();
  } // Use ThreadSleeper.INSTANCE instead.

  @VisibleForTesting
  public LightThreadSleeper(Sleeper internalSleeper, Clock clock) {
    mInternalSleeper = internalSleeper;
    mClock = clock;
  }

  @Override
  public void sleep(Duration duration) throws InterruptedException {
    mInternalSleeper.sleep(duration);
  }

  @Override
  public void sleep(Supplier<Duration> newIntervalSupplier) throws InterruptedException {
    Duration duration = newIntervalSupplier.get();
    if (duration.toMillis() < 0) {
      return;
    }
    if (duration.toMillis() < LIGHT_SLEEP_INTERVAL_MS) {
      sleep(duration);
      return;
    }
    long startSleepMs = mClock.millis();
    long sleepTo = startSleepMs + duration.toMillis();
    long timeNow;
    while ((timeNow = mClock.millis()) < sleepTo) {
      // TODO(baoloongmao): Make sure we need to config it.
      mInternalSleeper.sleep(Duration.ofMillis(sleepTo - timeNow > LIGHT_SLEEP_INTERVAL_MS
          ? LIGHT_SLEEP_INTERVAL_MS : sleepTo - timeNow));

      long newInterval = newIntervalSupplier.get().toMillis();
      if (newInterval >= 0) {
        sleepTo = startSleepMs + newInterval;
      }
    }
  }
}
