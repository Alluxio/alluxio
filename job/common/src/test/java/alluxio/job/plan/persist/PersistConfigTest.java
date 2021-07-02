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

package alluxio.job.plan.persist;

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Test {@link PersistConfig}.
 */
public final class PersistConfigTest {
  @Test
  public void jsonTest() throws Exception {
    PersistConfig config = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    PersistConfig other = mapper.readValue(mapper.writeValueAsString(config), PersistConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new PersistConfig(null, -1, true, "");
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The file path cannot be null", exception.getMessage());
    }
  }

  public static PersistConfig createRandom() {
    Random random = new Random();
    String path = "/" + CommonUtils.randomAlphaNumString(random.nextInt(10));
    String ufsPath = "/" + CommonUtils.randomAlphaNumString(random.nextInt(10));
    long mountId = random.nextLong();
    return new PersistConfig(path, mountId, random.nextBoolean(), ufsPath);
  }

  public void checkEquality(PersistConfig a, PersistConfig b) {
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a.getUfsPath(), b.getUfsPath());
    Assert.assertEquals(a.getMountId(), b.getMountId());
    Assert.assertEquals(a.isOverwrite(), b.isOverwrite());
    Assert.assertEquals(a, b);
  }
}
