/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.retry;

import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

/**
 * Each retry will cause a sleep to happen. This sleep will grow over time exponentially so each
 * sleep gets much larger than the last. To make sure that this growth does not grow out of control,
 * a max sleep is used as a bounding.
 */
@NotThreadSafe
public class ExponentialBackoffRetry extends SleepingRetry {
  private final Random mRandom = new Random();
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
    int count = getRetryCount();
    if (count >= 30) {
      // current logic overflows at 30, so set value to max
      return mMaxSleepMs;
    } else {
      int sleepMs = mBaseSleepTimeMs * Math.max(1, mRandom.nextInt(1 << (count + 1)));
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
