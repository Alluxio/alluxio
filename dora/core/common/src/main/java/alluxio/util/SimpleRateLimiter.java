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

package alluxio.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;

import java.time.Duration;
import java.util.Optional;

/**
 * A basic implementation of {@link RateLimiter}.
 */
public class SimpleRateLimiter implements RateLimiter {

  final Ticker mTicker;
  final long mMinDuration;

  long mLastAcquire = 0;

  SimpleRateLimiter(long permitsPerSecond) {
    this(permitsPerSecond, new Ticker() {
      @Override
      public long read() {
        return System.nanoTime();
      }
    });
  }

  /**
   * Creates a simple rate limiter for testing purpose.
   * @param permitsPerSecond permits per second
   * @param ticker the ticker
   */
  @VisibleForTesting
  public SimpleRateLimiter(long permitsPerSecond, Ticker ticker) {
    mTicker = ticker;
    mMinDuration = Duration.ofSeconds(1).toNanos() / permitsPerSecond;
  }

  @Override
  public long getWaitTimeNanos(long permit) {
    return permit - mTicker.read();
  }

  @Override
  public Optional<Long> acquire() {
    long nxtElapsed = mTicker.read();
    if (nxtElapsed - mLastAcquire >= mMinDuration) {
      mLastAcquire = nxtElapsed;
      return Optional.empty();
    }
    mLastAcquire += mMinDuration;
    return Optional.of(mLastAcquire);
  }
}
