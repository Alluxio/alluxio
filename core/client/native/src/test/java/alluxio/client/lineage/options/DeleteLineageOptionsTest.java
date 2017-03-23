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

package alluxio.client.lineage.options;

import alluxio.CommonTestUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link DeleteLineageOptions}.
 */
public final class DeleteLineageOptionsTest {

  /**
   * Tests that building a {@link DeleteLineageOptions} works.
   */
  @Test
  public void builder() {
    DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
    Assert.assertTrue(options.isCascade());
  }

  /**
   * Tests that building a {@link DeleteLineageOptions} with the defaults works.
   */
  @Test
  public void defaults() {
    DeleteLineageOptions options = DeleteLineageOptions.defaults();
    Assert.assertFalse(options.isCascade());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(DeleteLineageOptions.class);
  }
}
