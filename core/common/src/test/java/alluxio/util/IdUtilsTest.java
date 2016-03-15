/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link IdUtils}.
 */
public final class IdUtilsTest {
  /**
   * Tests if output of {@link IdUtils#getRandomNonNegativeLong()} is non-negative.
   * Also tests for randomness property.
   */
  @Test
  public void getRandomNonNegativeLongTest() throws Exception {
    long first = IdUtils.getRandomNonNegativeLong();
    long second = IdUtils.getRandomNonNegativeLong();
    Assert.assertTrue(first >= 0);
    Assert.assertTrue(second >= 0);
    Assert.assertTrue(first != second);
  }
}
