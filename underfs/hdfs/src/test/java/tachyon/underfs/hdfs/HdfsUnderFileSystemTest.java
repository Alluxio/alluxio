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

package tachyon.underfs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem.UnderFSType;

/**
 * Tests {@link HdfsUnderFileSystem}.
 */
public final class HdfsUnderFileSystemTest {

  private HdfsUnderFileSystem mMockHdfsUnderFileSystem;

  @Before
  public final void before() throws Exception {
    mMockHdfsUnderFileSystem = new HdfsUnderFileSystem("file:///", new TachyonConf(), null);
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
   * {@link HdfsUnderFileSystem#prepareConfiguration(String, TachyonConf, Configuration)} method.
   *
   * Checks the hdfs implements class and tachyon underfs config setting
   *
   * @throws Exception
   */
  @Test
  public void prepareConfigurationTest() throws Exception {
    TachyonConf tConf = new TachyonConf();
    Configuration hConf = new Configuration();
    mMockHdfsUnderFileSystem.prepareConfiguration("", tConf, hConf);
    Assert.assertEquals("org.apache.hadoop.hdfs.DistributedFileSystem", hConf.get("fs.hdfs.impl"));
    Assert.assertFalse(hConf.getBoolean("fs.hdfs.impl.disable.cache", false));
    Assert.assertNotNull(hConf.get(Constants.UNDERFS_HDFS_CONFIGURATION));
  }
}
