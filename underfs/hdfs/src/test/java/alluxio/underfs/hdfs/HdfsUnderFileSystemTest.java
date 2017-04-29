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

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;

/**
 * Tests {@link HdfsUnderFileSystem}.
 */
public final class HdfsUnderFileSystemTest {

  private HdfsUnderFileSystem mHdfsUnderFileSystem;

  @Before
  public final void before() throws Exception {
    mHdfsUnderFileSystem = HdfsUnderFileSystem.createInstance(new AlluxioURI("file:///"),
        new UnderFileSystemConfiguration(null));
  }

  /**
   * Tests the {@link HdfsUnderFileSystem#getUnderFSType()} method. Confirm the UnderFSType for
   * HdfsUnderFileSystem.
   */
  @Test
  public void getUnderFSType() throws Exception {
    Assert.assertEquals("hdfs", mHdfsUnderFileSystem.getUnderFSType());
  }

  /**
   * Tests the {@link HdfsUnderFileSystem#createConfiguration} method.
   *
   * Checks the hdfs implements class and alluxio underfs config setting
   */
  @Test
  public void prepareConfiguration() throws Exception {
    org.apache.hadoop.conf.Configuration conf =
        HdfsUnderFileSystem.createConfiguration(new UnderFileSystemConfiguration(null));
    Assert.assertEquals("org.apache.hadoop.hdfs.DistributedFileSystem", conf.get("fs.hdfs.impl"));
    Assert.assertTrue(conf.getBoolean("fs.hdfs.impl.disable.cache", false));
  }

  /**
   * Tests the HDFS client caching is disabled.
   */
  @Ignore // TODO(jiri): This test is failing for hadoop-1 profile. Re-enable after it is fixed.
  @Test
  public void disableHdfsCache() throws Exception {
    // create a default hadoop configuration
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    String underfsAddress = "hdfs://localhost";
    URI uri = new URI(underfsAddress);
    org.apache.hadoop.fs.FileSystem hadoopFs = org.apache.hadoop.fs.FileSystem.get(uri, hadoopConf);
    // use the replication to test the default value
    Assert.assertEquals("3", hadoopFs.getConf().get("dfs.replication"));

    // create a new configuration with updated dfs replication value
    UnderFileSystemConfiguration hadoopConf1 =
        new UnderFileSystemConfiguration(ImmutableMap.of("dfs.replication", "1"));
    HdfsUnderFileSystem hdfs =
        HdfsUnderFileSystem.createInstance(new AlluxioURI(underfsAddress), hadoopConf1);
    Assert.assertEquals("1",
        ((org.apache.hadoop.conf.Configuration) hdfs.getConf()).get("dfs.replication"));
  }
}
