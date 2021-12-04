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

package alluxio.job.plan.replicate;

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Test {@link ReplicateConfig}.
 */
public final class ReplicateConfigTest {
  @Test
  public void json() throws Exception {
    ReplicateConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    ReplicateConfig other =
        mapper.readValue(mapper.writeValueAsString(config), ReplicateConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void negativeReplicateNumber() {
    try {
      new ReplicateConfig("", 0, -1);
      Assert.fail("Cannot create ReplicateConfig with negative replicateNumber");
    } catch (IllegalArgumentException exception) {
      // expected exception thrown. test passes
    }
  }

  public void checkEquality(ReplicateConfig a, ReplicateConfig b) {
    Assert.assertEquals(a.getBlockId(), b.getBlockId());
    Assert.assertEquals(a.getReplicas(), b.getReplicas());
    Assert.assertEquals(a, b);
  }

  public static ReplicateConfig createRandom() {
    Random random = new Random();
    String path = "/" + CommonUtils.randomAlphaNumString(random.nextInt(10) + 1);
    ReplicateConfig config =
        new ReplicateConfig(path, random.nextLong(), random.nextInt(Integer.MAX_VALUE) + 1);
    return config;
  }
}
