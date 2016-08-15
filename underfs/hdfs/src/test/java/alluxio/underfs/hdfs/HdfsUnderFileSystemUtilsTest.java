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

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link HdfsUnderFileSystemUtils}.
 */
public final class HdfsUnderFileSystemUtilsTest {

  /**
   * Tests {@link HdfsUnderFileSystemUtils#addKey} method when a property is set in Alluxio.
   */
  @Test
  public void addKeyFromAlluxioConf() {
    PropertyKey key = PropertyKey.HOME;
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    Configuration.set(key, "alluxioKey");

    HdfsUnderFileSystemUtils.addKey(conf, key);
    Assert.assertEquals("alluxioKey", conf.get(key.toString()));

    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests {@link HdfsUnderFileSystemUtils#addKey} method when a property is set in system property.
   */
  @Test
  public void addKeyFromSystemProperty() {
    PropertyKey key = PropertyKey.HOME;
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

    System.setProperty(key.toString(), "systemKey");
    ConfigurationTestUtils.resetConfiguration();
    HdfsUnderFileSystemUtils.addKey(conf, key);
    Assert.assertEquals("systemKey", conf.get(key.toString()));

    System.clearProperty(key.toString());
    ConfigurationTestUtils.resetConfiguration();
  }
}
