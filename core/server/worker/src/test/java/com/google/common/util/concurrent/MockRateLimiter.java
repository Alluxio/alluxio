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

package com.google.common.util.concurrent;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.util.concurrent.RateLimiter.SleepingStopwatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Provides a fake {@link RateLimiter} to facilitate testing. Note that in Guava 14.0 the
 * {@link RateLimiter.SleepingStopwatch} class is package private, so this fake rate limiter has to
 * be in the same package to be able to extend it. Later versions of Guava makes it public, so this
 * can be moved to a package belongs to alluxio when updating to a newer version of Guava.
 */
public final class MockRateLimiter {
  private final RateLimiter mRateLimiter;
  private final FakeSleepingTicker mTicker;

  /**
   * Constructs {@link MockRateLimiter}.
   *
   * @param permitsPerSecond the rate of the returned {@code RateLimiter}, measured in
   *        how many permits become available per second
   */
  public MockRateLimiter(double permitsPerSecond) {
    mTicker = new FakeSleepingTicker();
    mRateLimiter = RateLimiter.create(permitsPerSecond, mTicker);
  }

  /**
   * Gets Guava RateLimiter.
   *
   * @return Guava RateLimiter
   */
  public RateLimiter getGuavaRateLimiter() {
    return mRateLimiter;
  }

  /**
   * Sleeps in milliseconds.
   *
   * @param millis the number of milliseconds
   */
  public void sleepMillis(int millis) {
    mTicker.sleepMillis(millis);
  }

  /**
   * Reads events and clears.
   * @return the list of events
   */
  public List<String> readEventsAndClear() {
    return mTicker.readEventsAndClear();
  }

  /**
   * The sleeping ticker gathers events and presents them as strings.
   * R0.6 means a delay of 0.6 seconds caused by the (R)ateLimiter
   * U1.0 means the (U)ser caused the ticker to sleep for a second.
   */
  private static class FakeSleepingTicker extends SleepingStopwatch {
    private long mInstantMicros = 0L;
    private final List<String> mEvents = new ArrayList<>();

    @Override
    public long readMicros() {
      return mInstantMicros;
    }

    private void sleepMillis(int millis) {
      sleepMicros("U", MILLISECONDS.toMicros(millis));
    }

    private void sleepMicros(String caption, long micros) {
      mInstantMicros += micros;
      mEvents.add(caption + String.format(Locale.ROOT, "%3.2f", (micros / 1000000.0)));
    }

    @Override
    public void sleepMicrosUninterruptibly(long micros) {
      sleepMicros("R", micros);
    }

    private List<String> readEventsAndClear() {
      try {
        return new ArrayList<>(mEvents);
      } finally {
        mEvents.clear();
      }
    }

    @Override
    public String toString() {
      return mEvents.toString();
    }
  }
}
