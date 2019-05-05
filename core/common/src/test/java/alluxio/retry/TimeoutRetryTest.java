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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    assertEquals(0, timeoutRetry.getAttemptCount());
    long startMs = System.currentTimeMillis();
    while (timeoutRetry.attempt()) {
      attempts++;
    }
    long endMs = System.currentTimeMillis();
    assertTrue(attempts > 0);
    assertTrue((endMs - startMs) >= timeoutMs);
    assertEquals(attempts, timeoutRetry.getAttemptCount());
    assertTrue(attempts <= (timeoutMs / sleepMs) + 1);
  }
}
