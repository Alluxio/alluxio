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

package alluxio.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.util.CommonUtils;

import org.junit.Test;

/**
 * Tests for the {@link ExponentialTimer}.
 */
public class ExponentialTimerTest {

  /**
   * Tests that the maximum total wait time is respected.
   */
  @Test
  public void expiration() {
    int maxTotalWaitTimeMs = 1000;
    ExponentialTimer timer = new ExponentialTimer(0, 0, 0, maxTotalWaitTimeMs);
    assertEquals(ExponentialTimer.Result.READY, timer.tick());
    CommonUtils.sleepMs(maxTotalWaitTimeMs);
    assertEquals(ExponentialTimer.Result.EXPIRED, timer.tick());
  }

  /**
   * Tests the exponential back-off logic.
   */
  @Test(timeout = 2000)
  public void backoff() {
    int n = 10;
    ExponentialTimer timer = new ExponentialTimer(1, 1000, 0, 1000);
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      while (timer.tick() == ExponentialTimer.Result.NOT_READY) {
        CommonUtils.sleepMs(10);
      }
      long now = System.currentTimeMillis();
      assertTrue(now - start >= (1 << i - 1));
    }
  }
}
