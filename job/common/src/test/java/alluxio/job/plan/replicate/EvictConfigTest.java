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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Test {@link EvictConfig}.
 */
public final class EvictConfigTest {
  @Test
  public void json() throws Exception {
    EvictConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    EvictConfig other = mapper.readValue(mapper.writeValueAsString(config), EvictConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void negativeReplicas() {
    try {
      new EvictConfig("", 0, -1);
      Assert.fail("Cannot create EvictConfig with negative replicas");
    } catch (IllegalArgumentException e) {
      // expected exception thrown. test passes
    }
  }

  @Test
  public void zeroReplicas() {
    try {
      new EvictConfig("", 0, 0);
      Assert.fail("Cannot create EvictConfig with zero replicas");
    } catch (IllegalArgumentException e) {
      // expected exception thrown. test passes
    }
  }

  public void checkEquality(EvictConfig a, EvictConfig b) {
    Assert.assertEquals(a.getBlockId(), b.getBlockId());
    Assert.assertEquals(a.getReplicas(), b.getReplicas());
    Assert.assertEquals(a, b);
  }

  public static EvictConfig createRandom() {
    Random random = new Random();
    EvictConfig config = new EvictConfig("", random.nextLong(),
        random.nextInt(Integer.MAX_VALUE) + 1);
    return config;
  }
}
