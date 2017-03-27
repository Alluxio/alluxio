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

package alluxio.underfs.hdfs;

import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.SystemPropertyRule;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

/**
 * Tests {@link HdfsUnderFileSystemUtils}.
 */
public final class HdfsUnderFileSystemUtilsTest {

  /**
   * Tests {@link HdfsUnderFileSystemUtils#addKey} method when a property is set in Alluxio.
   */
  @Test
  public void addKeyFromAlluxioConf() throws Exception {
    PropertyKey key = PropertyKey.HOME;
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    try (Closeable c = new ConfigurationRule(key, "alluxioKey").toResource()) {
      HdfsUnderFileSystemUtils.addKey(conf, key);
      Assert.assertEquals("alluxioKey", conf.get(key.toString()));
    }
  }

  /**
   * Tests {@link HdfsUnderFileSystemUtils#addKey} method when a property is set in system property.
   */
  @Test
  public void addKeyFromSystemProperty() throws Exception {
    PropertyKey key = PropertyKey.HOME;
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    try (Closeable p = new SystemPropertyRule(key.toString(), "systemKey").toResource()) {
      ConfigurationTestUtils.resetConfiguration();  // ensure system property change take effect
      HdfsUnderFileSystemUtils.addKey(conf, key);
      Assert.assertEquals("systemKey", conf.get(key.toString()));
      ConfigurationTestUtils.resetConfiguration();
    }
  }
}
