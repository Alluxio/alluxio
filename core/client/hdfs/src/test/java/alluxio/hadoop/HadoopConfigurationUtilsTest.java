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
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.conf.Source;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link HadoopConfigurationUtils} class.
 */
public final class HadoopConfigurationUtilsTest {
  private static final String TEST_S3_ACCCES_KEY = "TEST ACCESS KEY";
  private static final String TEST_S3_SECRET_KEY = "TEST SECRET KEY";
  private static final String TEST_ALLUXIO_PROPERTY = "alluxio.unsupported.parameter";
  private static final String TEST_ALLUXIO_VALUE = "alluxio.unsupported.value";

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Test for the {@link HadoopConfigurationUtils#mergeHadoopConfiguration} method for an empty
   * configuration.
   */
  @Test
  public void mergeEmptyHadoopConfiguration() {
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    long beforeSize = Configuration.toMap().size();
    HadoopConfigurationUtils.mergeHadoopConfiguration(hadoopConfig, Configuration.global());
    long afterSize = Configuration.toMap().size();
    Assert.assertEquals(beforeSize, afterSize);
    Assert.assertFalse(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
  }

  /**
   * Test for the {@link HadoopConfigurationUtils#mergeHadoopConfiguration} method.
   */
  @Test
  public void mergeHadoopConfiguration() {
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set(PropertyKey.S3A_ACCESS_KEY.toString(), TEST_S3_ACCCES_KEY);
    hadoopConfig.set(PropertyKey.S3A_SECRET_KEY.toString(), TEST_S3_SECRET_KEY);
    hadoopConfig.set(TEST_ALLUXIO_PROPERTY, TEST_ALLUXIO_VALUE);
    hadoopConfig.setBoolean(PropertyKey.ZOOKEEPER_ENABLED.getName(), true);
    hadoopConfig.set(PropertyKey.ZOOKEEPER_ADDRESS.getName(),
        "host1:port1,host2:port2;host3:port3");

    // This hadoop config will not be loaded into Alluxio configuration.
    hadoopConfig.set("hadoop.config.parameter", "hadoop config value");
    HadoopConfigurationUtils.mergeHadoopConfiguration(hadoopConfig, Configuration.global());
    Assert.assertEquals(TEST_S3_ACCCES_KEY, Configuration.get(PropertyKey.S3A_ACCESS_KEY));
    Assert.assertEquals(TEST_S3_SECRET_KEY, Configuration.get(PropertyKey.S3A_SECRET_KEY));
    Assert.assertEquals(Source.RUNTIME, Configuration.getSource(PropertyKey.S3A_ACCESS_KEY));
    Assert.assertEquals(Source.RUNTIME, Configuration.getSource(PropertyKey.S3A_SECRET_KEY));
    Assert.assertTrue(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    Assert.assertEquals("host1:port1,host2:port2;host3:port3",
        Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS));
  }
}
