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

import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Each retry will cause a sleep to happen. This sleep will grow over time exponentially so each
 * sleep gets much larger than the last. To make sure that this growth does not grow out of control,
 * a max sleep is used as a bounding.
 */
@NotThreadSafe
public class ExponentialBackoffRetry extends SleepingRetry {
  private final int mBaseSleepTimeMs;
  private final int mMaxSleepMs;

  /**
   * Constructs a new retry facility which sleeps for an exponentially increasing amount of time
   * between retries.
   *
   * @param baseSleepTimeMs the sleep in milliseconds to begin with
   * @param maxSleepMs the max sleep in milliseconds as a bounding
   * @param maxRetries the max count of retries
   */
  public ExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepMs, int maxRetries) {
    super(maxRetries);
    Preconditions.checkArgument(baseSleepTimeMs >= 0, "Base must be a positive number, or 0");
    Preconditions.checkArgument(maxSleepMs >= 0, "Max must be a positive number, or 0");

    mBaseSleepTimeMs = baseSleepTimeMs;
    mMaxSleepMs = maxSleepMs;
  }

  @Override
  protected long getSleepTime() {
    int count = getAttemptCount();
    if (count >= 30) {
      // current logic overflows at 30, so set value to max
      return mMaxSleepMs;
    } else {
      // use randomness to avoid contention between many operations using the same retry policy
      int sleepMs =
          mBaseSleepTimeMs * (ThreadLocalRandom.current().nextInt(1 << count, 1 << (count + 1)));
      return Math.min(abs(sleepMs, mMaxSleepMs), mMaxSleepMs);
    }
  }

  private static int abs(int value, int defaultValue) {
    int result = Math.abs(value);
    if (result == Integer.MIN_VALUE) {
      result = defaultValue;
    }
    return result;
  }
}
