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

import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A retry policy which allows retrying until a specified timeout is reached.
 */
@NotThreadSafe
public class TimeoutRetry implements RetryPolicy {

  private final long mRetryTimeoutMs;
  private final long mSleepMs;
  private long mStartMs = 0;
  private int mAttemptCount = 0;

  /**
   * Constructs a retry facility which allows retrying until a specified timeout is reached.
   *
   * @param retryTimeoutMs maximum period of time to retry for, in milliseconds
   * @param sleepMs time in milliseconds to sleep before retrying
   */
  public TimeoutRetry(long retryTimeoutMs, int sleepMs) {
    Preconditions.checkArgument(retryTimeoutMs > 0, "Retry timeout must be a positive number");
    Preconditions.checkArgument(sleepMs >= 0, "sleepMs cannot be negative");
    mRetryTimeoutMs = retryTimeoutMs;
    mSleepMs = sleepMs;
  }

  @Override
  public int getAttemptCount() {
    return mAttemptCount;
  }

  @Override
  public boolean attempt() {
    if (mAttemptCount == 0) {
      // first attempt, set the start time
      mStartMs = CommonUtils.getCurrentMs();
      mAttemptCount++;
      return true;
    }
    if (mSleepMs > 0) {
      CommonUtils.sleepMs(mSleepMs);
    }
    if ((CommonUtils.getCurrentMs() - mStartMs) <= mRetryTimeoutMs) {
      mAttemptCount++;
      return true;
    }
    return false;
  }
}
