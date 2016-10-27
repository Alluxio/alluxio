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
 * Tests for the {@link GetLineageInfoListOptions} class.
 */
public class GetLineageInfoListOptionsTest {
  /**
   * Tests that the default for this option can successfully be built.
   */
  @Test
  public void defaults() {
    Assert.assertNotNull("The default options should not be null",
        GetLineageInfoListOptions.defaults());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(GetLineageInfoListOptions.class);
  }
}
