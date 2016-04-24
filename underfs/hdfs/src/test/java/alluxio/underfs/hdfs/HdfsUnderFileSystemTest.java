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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem.UnderFSType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link HdfsUnderFileSystem}.
 */
public final class HdfsUnderFileSystemTest {

  private HdfsUnderFileSystem mMockHdfsUnderFileSystem;

  @Before
  public final void before() throws Exception {
    mMockHdfsUnderFileSystem =
        new HdfsUnderFileSystem(new AlluxioURI("file:///"), new Configuration(), null);
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
   * Tests the {@link HdfsUnderFileSystem#prepareConfiguration} method.
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
