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

package alluxio.retry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link ExponentialBackoffRetry} class.
 */
public class ExponentialBackoffRetryTest {

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
      Assert.assertTrue("Time must always be positive: " + time, time > 0);
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
    public int getRetryCount() {
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
