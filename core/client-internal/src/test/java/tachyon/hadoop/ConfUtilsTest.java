/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Tests for the {@link ConfUtils} class.
 */
public final class ConfUtilsTest {
  private static final String TEST_S3_ACCCES_KEY = "TEST ACCESS KEY";
  private static final String TEST_S3_SECRET_KEY = "TEST SECRET KEY";
  private static final String TEST_WORKER_MEMORY_SIZE = Integer.toString(654321);

  /**
   * Test for the {@link ConfUtils#loadFromHadoopConfiguration(Configuration)} method for an empty
   * configuration.
   */
  @Test
  public void loadFromEmptyHadoopConfigurationTest() {
    Configuration hadoopConfig = new Configuration();
    TachyonConf tachyonConf = ConfUtils.loadFromHadoopConfiguration(hadoopConfig);
    Assert.assertEquals(0, tachyonConf.toMap().size());
  }

  /**
   * Test for the {@link ConfUtils#loadFromHadoopConfiguration(Configuration)} method.
   */
  @Test
  public void loadFromHadoopConfigurationTest() {
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.set(Constants.S3_ACCESS_KEY, TEST_S3_ACCCES_KEY);
    hadoopConfig.set(Constants.S3_SECRET_KEY, TEST_S3_SECRET_KEY);
    hadoopConfig.set(Constants.WORKER_MEMORY_SIZE, TEST_WORKER_MEMORY_SIZE);
    // This hadoop config will not be loaded into TachyonConf.
    hadoopConfig.set("hadoop.config.parameter", "hadoop config value");

    TachyonConf tachyonConf = ConfUtils.loadFromHadoopConfiguration(hadoopConfig);
    Assert.assertEquals(3, tachyonConf.toMap().size());
    Assert.assertEquals(TEST_S3_ACCCES_KEY, tachyonConf.get(Constants.S3_ACCESS_KEY));
    Assert.assertEquals(TEST_S3_SECRET_KEY, tachyonConf.get(Constants.S3_SECRET_KEY));
    Assert.assertEquals(TEST_WORKER_MEMORY_SIZE, tachyonConf.get(Constants.WORKER_MEMORY_SIZE));
  }
}
