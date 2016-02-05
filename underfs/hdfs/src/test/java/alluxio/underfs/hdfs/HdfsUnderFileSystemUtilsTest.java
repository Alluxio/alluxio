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

package alluxio.underfs.hdfs;

import org.junit.Assert;
import org.junit.Test;

import alluxio.Configuration;

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
