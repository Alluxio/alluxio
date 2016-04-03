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

package alluxio.underfs.hdfs;

import alluxio.Configuration;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link HdfsUnderFileSystemUtils}.
 */
public final class HdfsUnderFileSystemUtilsTest {

  /**
   * Tests the {@link HdfsUnderFileSystemUtils#addKey} method.
   */
  @Test
  public void addKeyTest() {
    String key = "key";
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    Configuration configuration = new Configuration();
    configuration.set(key, "alluxioKey");

    System.setProperty(key, "systemKey");
    HdfsUnderFileSystemUtils.addKey(conf, configuration, key);
    Assert.assertEquals("systemKey", conf.get(key));

    System.clearProperty(key);
    HdfsUnderFileSystemUtils.addKey(conf, configuration, key);
    Assert.assertEquals("alluxioKey", conf.get(key));
  }
}
