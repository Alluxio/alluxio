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

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A retry policy that uses thread sleeping for the delay.
 */
@NotThreadSafe
public abstract class SleepingRetry implements RetryPolicy {
  private final int mMaxRetries;
  private int mAttemptCount = 0;

  protected SleepingRetry(int maxRetries) {
    Preconditions.checkArgument(maxRetries > 0, "Max retries must be a positive number");
    mMaxRetries = maxRetries;
  }

  @Override
  public int getAttemptCount() {
    return mAttemptCount;
  }

  @Override
  public boolean attempt() {
    if (mAttemptCount <= mMaxRetries) {
      if (mAttemptCount == 0) {
        // first attempt, do not sleep
        mAttemptCount++;
        return true;
      }
      try {
        getSleepUnit().sleep(getSleepTime());
        mAttemptCount++;
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  /**
   * Unit of time that {@link #getSleepTime()} is measured in. Defaults to
   * {@link java.util.concurrent.TimeUnit#MILLISECONDS}.
   */
  protected TimeUnit getSleepUnit() {
    return TimeUnit.MILLISECONDS;
  }

  /**
   * How long to sleep before the next retry is performed. This method is used with
   * {@link #getSleepUnit()}, so all time given here must match the unit provided.
   */
  protected abstract long getSleepTime();
}
