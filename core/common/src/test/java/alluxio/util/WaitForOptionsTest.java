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

package alluxio.util;

import static org.junit.Assert.assertEquals;

import alluxio.test.util.CommonUtils;

import org.junit.Test;

import java.util.Random;

public final class WaitForOptionsTest {
  @Test
  public void defaults() {
    WaitForOptions options = WaitForOptions.defaults();
    assertEquals(WaitForOptions.DEFAULT_INTERVAL, options.getInterval());
    assertEquals(WaitForOptions.NEVER, options.getTimeoutMs());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    int interval = random.nextInt();
    int timeout = random.nextInt();

    WaitForOptions options = WaitForOptions.defaults();
    options.setInterval(interval);
    options.setTimeoutMs(timeout);

    assertEquals(interval, options.getInterval());
    assertEquals(timeout, options.getTimeoutMs());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(WaitForOptions.class);
  }
}
