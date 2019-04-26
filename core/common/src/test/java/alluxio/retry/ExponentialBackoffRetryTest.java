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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link ExponentialBackoffRetry} class.
 */
public final class ExponentialBackoffRetryTest {

  /**
   * Ensures that a lot of retries always produce a positive time.
   */
  @Test
  public void largeRetriesProducePositiveTime() {
    int max = 1000;
    MockExponentialBackoffRetry backoff =
        new MockExponentialBackoffRetry(50, Integer.MAX_VALUE, max);
    for (int i = 0; i < max; i++) {
      backoff.setRetryCount(i);
      long time = backoff.getSleepTime();
      assertTrue("Time must always be positive: " + time, time > 0);
    }
  }

  /**
   * Mocks the {@link ExponentialBackoffRetry} class.
   */
  public static final class MockExponentialBackoffRetry extends ExponentialBackoffRetry {
    private int mRetryCount = 0;

    /**
     * Constructs a new mock.
     *
     * @param baseSleepTimeMs the basic sleep time in milliseconds
     * @param maxSleepMs the max sleep time in milliseconds
     * @param maxRetries the max count of retries
     */
    public MockExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepMs, int maxRetries) {
      super(baseSleepTimeMs, maxSleepMs, maxRetries);
    }

    @Override
    public int getAttemptCount() {
      return mRetryCount;
    }

    /**
     * Sets the count of retries.
     *
     * @param retryCount the count of retries
     */
    public void setRetryCount(int retryCount) {
      mRetryCount = retryCount;
    }
  }
}
