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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link TimeoutRetry} class.
 */
public final class TimeoutRetryTest {

  /**
   * Tests that the provided timeout is respected.
   */
  @Test
  public void timeout() {
    final long timeoutMs = 50;
    final int sleepMs = 10;
    int attempts = 0;
    TimeoutRetry timeoutRetry = new TimeoutRetry(timeoutMs, sleepMs);
    Assert.assertEquals(0, timeoutRetry.getRetryCount());
    long startMs = System.currentTimeMillis();
    while (timeoutRetry.attemptRetry()) {
      attempts++;
    }
    long endMs = System.currentTimeMillis();
    Assert.assertTrue(attempts > 0);
    Assert.assertTrue((endMs - startMs) >= timeoutMs);
    Assert.assertEquals(attempts, timeoutRetry.getRetryCount());
    Assert.assertTrue(attempts <= (timeoutMs / sleepMs) + 1);
  }
}
