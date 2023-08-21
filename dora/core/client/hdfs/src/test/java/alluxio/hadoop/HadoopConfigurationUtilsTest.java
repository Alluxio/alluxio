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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import org.junit.Test;

/**
 * Tests for the {@link HadoopConfigurationUtils} class.
 */
public final class HadoopConfigurationUtilsTest {
  private static final String TEST_S3_ACCCES_KEY = "TEST ACCESS KEY";
  private static final String TEST_S3_SECRET_KEY = "TEST SECRET KEY";
  private static final String TEST_ALLUXIO_PROPERTY = "alluxio.unsupported.parameter";
  private static final String TEST_ALLUXIO_VALUE = "alluxio.unsupported.value";
  private InstancedConfiguration mConf = Configuration.copyGlobal();

  /**
   * Test for the {@link HadoopConfigurationUtils#getConfigurationFromHadoop} method for an empty
   * configuration.
   */
  @Test
  public void mergeEmptyHadoopConfiguration() {
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    mConf.merge(
        HadoopConfigurationUtils.getConfigurationFromHadoop(hadoopConfig), Source.RUNTIME);
    assertFalse(mConf.getBoolean(PropertyKey.MASTER_HOSTNAME));
  }

  /**
   * Test for the {@link HadoopConfigurationUtils#getConfigurationFromHadoop} method.
   */
  @Test
  public void mergeHadoopConfiguration() {
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set(PropertyKey.S3A_ACCESS_KEY.toString(), TEST_S3_ACCCES_KEY);
    hadoopConfig.set(PropertyKey.S3A_SECRET_KEY.toString(), TEST_S3_SECRET_KEY);
    hadoopConfig.set(TEST_ALLUXIO_PROPERTY, TEST_ALLUXIO_VALUE);

    // This hadoop config will not be loaded into Alluxio configuration.
    hadoopConfig.set("hadoop.config.parameter", "hadoop config value");
    mConf.merge(
        HadoopConfigurationUtils.getConfigurationFromHadoop(hadoopConfig), Source.RUNTIME);
    assertEquals(TEST_S3_ACCCES_KEY, mConf.get(PropertyKey.S3A_ACCESS_KEY));
    assertEquals(TEST_S3_SECRET_KEY, mConf.get(PropertyKey.S3A_SECRET_KEY));
    assertEquals(Source.RUNTIME, mConf.getSource(PropertyKey.S3A_ACCESS_KEY));
    assertEquals(Source.RUNTIME, mConf.getSource(PropertyKey.S3A_SECRET_KEY));
  }
}
