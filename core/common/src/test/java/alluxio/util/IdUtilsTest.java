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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;

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
  public void getRandomNonNegativeLong() throws Exception {
    long first = IdUtils.getRandomNonNegativeLong();
    long second = IdUtils.getRandomNonNegativeLong();
    assertTrue(first >= 0);
    assertTrue(second >= 0);
    assertTrue(first != second);
  }

  /**
   * Tests if output of {@link IdUtils#createFileId(long)} is valid.
   */
  @Test
  public void createFileId() throws Exception {
    long containerId = 1;
    long fileId = IdUtils.createFileId(containerId);
    assertNotEquals(-1, fileId);
  }

  /**
   * Tests if output of {@link IdUtils#createRpcId()} is non-empty.
   * Also tests for randomness property.
   */
  @Test
  public void createRpcId() throws Exception {
    String first = IdUtils.createRpcId();
    assertTrue(!first.isEmpty());
    String second = IdUtils.createRpcId();
    assertTrue(!second.isEmpty());
    assertNotEquals(first, second);
  }
}
