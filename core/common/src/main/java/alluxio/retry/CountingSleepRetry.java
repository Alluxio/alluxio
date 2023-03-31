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
 * An option which allows retrying with sleeping in between the attempts.
 * The difference between {@code CountingSleepRetry(5, 1000)} and {@code TimeoutRetry(5000, 1000)}
 * is that CountingSleepRetry will ignore the wall clock and JVM pause time, and wait for 5s
 * in user time, but TimeoutRetry only waits 5s in wall clock time.
 *
 * So if you want to wait for
 */
@NotThreadSafe
public class CountingSleepRetry implements RetryPolicy {

  private final int mMaxRetries;
  private final int mSleepMs;
  private int mAttemptCount = 0;

  /**
   * Constructs a retry facility which allows max number of retries.
   *
   * @param maxRetries max number of retries
   */
  public CountingSleepRetry(int maxRetries, int sleepMs) {
    Preconditions.checkArgument(maxRetries >= 0, "Max retries must be a non-negative number");
    Preconditions.checkArgument(sleepMs > 0, "sleepMs should be greater than zero");
    mMaxRetries = maxRetries;
    mSleepMs = sleepMs;
  }

  @Override
  public int getAttemptCount() {
    return mAttemptCount;
  }

  @Override
  public boolean attempt() {
    if (mAttemptCount <= mMaxRetries) {
      mAttemptCount++;
      CommonUtils.sleepMs(mSleepMs);
      return true;
    }
    return false;
  }

  /**
   * Reset the count of retries.
   */
  public void reset() {
    mAttemptCount = 0;
  }
}

