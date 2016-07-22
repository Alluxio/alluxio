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

package alluxio.hadoop;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests for the {@link ConfUtils} class.
 */
public final class ConfUtilsTest {
  private static final String TEST_S3_ACCCES_KEY = "TEST ACCESS KEY";
  private static final String TEST_S3_SECRET_KEY = "TEST SECRET KEY";

  /**
   * Test for the {@link ConfUtils#mergeHadoopConfiguration} method for an empty configuration.
   */
  @Test
  public void mergeEmptyHadoopConfigurationTest() {
    Configuration.defaultInit();
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();

    Map<String, String> before = Configuration.toMap();
    ConfUtils.mergeHadoopConfiguration(hadoopConfig);
    Map<String, String> after = Configuration.toMap();
    Assert.assertEquals(before.size(), after.size());
  }

  /**
   * Test for the {@link ConfUtils#mergeHadoopConfiguration} method.
   */
  @Test
  public void mergeHadoopConfigurationTest() {
    Configuration.defaultInit();
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set(PropertyKey.S3N_ACCESS_KEY.toString(), TEST_S3_ACCCES_KEY);
    hadoopConfig.set(PropertyKey.S3N_SECRET_KEY.toString(), TEST_S3_SECRET_KEY);

    // This hadoop config will not be loaded into Alluxio configuration.
    hadoopConfig.set("hadoop.config.parameter", "hadoop config value");

    Map<String, String> before = Configuration.toMap();
    ConfUtils.mergeHadoopConfiguration(hadoopConfig);
    Map<String, String> after = Configuration.toMap();
    Assert.assertEquals(before.size() + 3, after.size());
    Assert.assertEquals(TEST_S3_ACCCES_KEY, Configuration.get(PropertyKey.S3N_ACCESS_KEY));
    Assert.assertEquals(TEST_S3_SECRET_KEY, Configuration.get(PropertyKey.S3N_SECRET_KEY));
  }
}
