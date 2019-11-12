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

package alluxio.job.util;

import alluxio.job.TestPlanConfig;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the utility class {@link SerializationUtils}.
 */
public final class SerializationUtilsTest {
  @Test
  public void basicTest() throws Exception {
    TestPlanConfig config = new TestPlanConfig("test");
    byte[] bytes = SerializationUtils.serialize(config);
    Object deserialized = SerializationUtils.deserialize(bytes);
    Assert.assertTrue(deserialized instanceof TestPlanConfig);
    Assert.assertEquals(config, deserialized);
  }
}
