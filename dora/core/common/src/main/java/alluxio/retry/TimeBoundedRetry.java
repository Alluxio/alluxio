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

package alluxio.retry;

import alluxio.time.Sleeper;
import alluxio.time.TimeContext;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Retry mechanism which performs retries until a certain period of time has elapsed. Subclasses
 * determine the interval between retries.
 */
public abstract class TimeBoundedRetry implements RetryPolicy {
  private final Clock mClock;
  private final Sleeper mSleeper;
  private final Duration mMaxDuration;
  private final Instant mStartTime;
  private final Instant mEndTime;

  private int mAttemptCount = 0;

  /**
   * @param timeCtx the time context to use for time-based operations
   * @param maxDuration the maximum duration
   */
  public TimeBoundedRetry(TimeContext timeCtx, Duration maxDuration) {
    mClock = timeCtx.getClock();
    mSleeper = timeCtx.getSleeper();
    mMaxDuration = maxDuration;
    mStartTime = mClock.instant();
    mEndTime = mStartTime.plus(mMaxDuration);
  }

  @Override
  public int getAttemptCount() {
    return mAttemptCount;
  }

  @Override
  public boolean attempt() {
    if (mAttemptCount == 0) {
      mAttemptCount++;
      return true;
    }
    Instant now = mClock.instant();
    if (now.isAfter(mEndTime) || now.equals(mEndTime)) {
      return false;
    }
    Duration nextWaitTime = computeNextWaitTime();
    if (now.plus(nextWaitTime).isAfter(mEndTime)) {
      nextWaitTime = Duration.between(now, mEndTime);
    }
    if (nextWaitTime.getNano() > 0) {
      try {
        mSleeper.sleep(nextWaitTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    mAttemptCount++;
    return true;
  }

  /**
   * @return how long to wait before the next retry
   */
  protected abstract Duration computeNextWaitTime();
}
