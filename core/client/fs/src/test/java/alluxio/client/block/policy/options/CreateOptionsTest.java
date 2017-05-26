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

package alluxio.client.block.policy.options;

import alluxio.CommonTestUtils;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Tests for the {@link CreateOptions} class.
 */
public final class CreateOptionsTest {
  /**
   * Tests for defaults {@link CreateOptions}.
   */
  @Test
  public void defaults() throws IOException {
    CreateOptions options = CreateOptions.defaults();

    Assert.assertEquals(1, options.getDeterministicHashPolicyNumShards());
    Assert.assertEquals(null, options.getLocationPolicyClassName());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    int deterministicHashPolicyNumShards = random.nextInt();
    String locationPolicyClassName = CommonUtils.randomAlphaNumString(10);

    CreateOptions options = CreateOptions.defaults();
    options.setDeterministicHashPolicyNumShards(deterministicHashPolicyNumShards);
    options.setLocationPolicyClassName(locationPolicyClassName);
    Assert.assertEquals(deterministicHashPolicyNumShards,
        options.getDeterministicHashPolicyNumShards());
    Assert.assertEquals(locationPolicyClassName, options.getLocationPolicyClassName());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CreateOptions.class);
  }
}
