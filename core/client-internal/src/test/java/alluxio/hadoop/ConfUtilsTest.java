/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import alluxio.Configuration;
import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link ConfUtils} class.
 */
public final class ConfUtilsTest {
  private static final String TEST_S3_ACCCES_KEY = "TEST ACCESS KEY";
  private static final String TEST_S3_SECRET_KEY = "TEST SECRET KEY";
  private static final String TEST_WORKER_MEMORY_SIZE = Integer.toString(654321);

  /**
   * Test for the {@link ConfUtils#loadFromHadoopConfiguration} method for an empty configuration.
   */
  @Test
  public void loadFromEmptyHadoopConfigurationTest() {
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    Configuration configuration = ConfUtils.loadFromHadoopConfiguration(hadoopConfig);
    Assert.assertEquals(0, configuration.toMap().size());
  }

  /**
   * Test for the {@link ConfUtils#loadFromHadoopConfiguration} method.
   */
  @Test
  public void loadFromHadoopConfigurationTest() {
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set(Constants.S3_ACCESS_KEY, TEST_S3_ACCCES_KEY);
    hadoopConfig.set(Constants.S3_SECRET_KEY, TEST_S3_SECRET_KEY);
    hadoopConfig.set(Constants.WORKER_MEMORY_SIZE, TEST_WORKER_MEMORY_SIZE);
    // This hadoop config will not be loaded into Alluxio configuration.
    hadoopConfig.set("hadoop.config.parameter", "hadoop config value");

    Configuration configuration = ConfUtils.loadFromHadoopConfiguration(hadoopConfig);
    Assert.assertEquals(3, configuration.toMap().size());
    Assert.assertEquals(TEST_S3_ACCCES_KEY, configuration.get(Constants.S3_ACCESS_KEY));
    Assert.assertEquals(TEST_S3_SECRET_KEY, configuration.get(Constants.S3_SECRET_KEY));
    Assert.assertEquals(TEST_WORKER_MEMORY_SIZE, configuration.get(Constants.WORKER_MEMORY_SIZE));
  }
}
