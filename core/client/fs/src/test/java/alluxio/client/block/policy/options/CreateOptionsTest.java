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

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

import com.google.common.testing.EqualsTester;
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
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    CreateOptions options = CreateOptions.defaults(conf);

    Assert.assertEquals(conf
        .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS),
        options.getDeterministicHashPolicyNumShards());
    Assert.assertEquals(conf.get(PropertyKey.USER_BLOCK_WRITE_LOCATION_POLICY),
        options.getLocationPolicyClassName());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    int deterministicHashPolicyNumShards = random.nextInt();
    String locationPolicyClassName = CommonUtils.randomAlphaNumString(10);

    CreateOptions options = CreateOptions.defaults(ConfigurationTestUtils.defaults());
    options.setDeterministicHashPolicyNumShards(deterministicHashPolicyNumShards);
    options.setLocationPolicyClassName(locationPolicyClassName);
    Assert.assertEquals(deterministicHashPolicyNumShards,
        options.getDeterministicHashPolicyNumShards());
    Assert.assertEquals(locationPolicyClassName, options.getLocationPolicyClassName());
  }

  @Test
  public void equalsTest() throws Exception {
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    new EqualsTester()
        .addEqualityGroup(CreateOptions.defaults(conf), CreateOptions.defaults(conf))
        .addEqualityGroup(CreateOptions.defaults(conf).setBlockReservedCapacity(1),
            CreateOptions.defaults(conf).setBlockReservedCapacity(1))
        .addEqualityGroup(CreateOptions.defaults(conf).setDeterministicHashPolicyNumShards(5),
            CreateOptions.defaults(conf).setDeterministicHashPolicyNumShards(5))
        .addEqualityGroup(CreateOptions.defaults(conf).setSpecificWorker("a"),
            CreateOptions.defaults(conf).setSpecificWorker("a"))
        .addEqualityGroup(CreateOptions.defaults(conf).setTieredIdentity(null),
            CreateOptions.defaults(conf).setTieredIdentity(null))
        .testEquals();
  }
}
