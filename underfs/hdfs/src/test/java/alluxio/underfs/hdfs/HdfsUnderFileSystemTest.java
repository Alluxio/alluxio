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
import org.junit.Before;
import org.junit.Test;

import alluxio.Constants;
import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem.UnderFSType;

/**
 * Tests {@link HdfsUnderFileSystem}.
 */
public final class HdfsUnderFileSystemTest {

  private HdfsUnderFileSystem mMockHdfsUnderFileSystem;

  @Before
  public final void before() throws Exception {
    mMockHdfsUnderFileSystem = new HdfsUnderFileSystem("file:///", new Configuration(), null);
  }

  /**
   * Tests the {@link HdfsUnderFileSystem#getUnderFSType()} method.
   * Confirm the UnderFSType for HdfsUnderFileSystem
   *
   * @throws Exception
   */
  @Test
  public void getUnderFSTypeTest() throws Exception {
    Assert.assertEquals(UnderFSType.HDFS, mMockHdfsUnderFileSystem.getUnderFSType());
  }

  /**
   * Tests the
   * {@link HdfsUnderFileSystem#prepareConfiguration(String, Configuration, org.apache.hadoop.conf.Configuration)} method.
   *
   * Checks the hdfs implements class and alluxio underfs config setting
   *
   * @throws Exception
   */
  @Test
  public void prepareConfigurationTest() throws Exception {
    Configuration tConf = new Configuration();
    org.apache.hadoop.conf.Configuration hConf = new org.apache.hadoop.conf.Configuration();
    mMockHdfsUnderFileSystem.prepareConfiguration("", tConf, hConf);
    Assert.assertEquals("org.apache.hadoop.hdfs.DistributedFileSystem", hConf.get("fs.hdfs.impl"));
    Assert.assertFalse(hConf.getBoolean("fs.hdfs.impl.disable.cache", false));
    Assert.assertNotNull(hConf.get(Constants.UNDERFS_HDFS_CONFIGURATION));
  }
}
