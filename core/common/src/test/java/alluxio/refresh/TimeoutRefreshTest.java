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

package alluxio.refresh;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.util.CommonUtils;

import org.junit.Test;

/**
 * Tests for the {@link TimeoutRefresh} class.
 */
public final class TimeoutRefreshTest {

  /**
   * Tests that the provided timeout is respected.
   */
  @Test
  public void timeout() {
    final long timeoutMs = 500;
    final long slackMs = 200;
    TimeoutRefresh timeoutRefresh = new TimeoutRefresh(timeoutMs);
    // First check, should attempt
    assertTrue(timeoutRefresh.attempt());
    // Second check, should not attempt before refresh timeout
    assertFalse(timeoutRefresh.attempt());

    CommonUtils.sleepMs(timeoutMs);
    CommonUtils.sleepMs(slackMs);

    assertTrue(timeoutRefresh.attempt());
    assertFalse(timeoutRefresh.attempt());
  }
}
