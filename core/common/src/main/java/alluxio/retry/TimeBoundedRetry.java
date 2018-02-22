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

  private int mRetryCount = 0;
  private boolean mDone = false;

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
    return mRetryCount;
  }

  @Override
  public boolean attempt() {
    if (mDone) {
      return false;
    }
    if (mRetryCount == 0) {
      mRetryCount++;
      return true;
    }
    Instant now = mClock.instant();
    // We should not do a retry if now == mEndTime. The final retry is timed to land at mEndTime,
    // so if now == mEndTime, the operation may have taken less than 1ms.
    if (!now.isBefore(mEndTime)) {
      mDone = true;
      return false;
    }
    try {
      Duration nextWaitTime = computeNextWaitTime();
      if (now.plus(nextWaitTime).isAfter(mEndTime)) {
        nextWaitTime = Duration.between(now, mEndTime);
        mDone = true;
      }
      mSleeper.sleep(nextWaitTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    mRetryCount++;
    return true;
  }

  /**
   * @return how long to wait before the next retry
   */
  protected abstract Duration computeNextWaitTime();
}
