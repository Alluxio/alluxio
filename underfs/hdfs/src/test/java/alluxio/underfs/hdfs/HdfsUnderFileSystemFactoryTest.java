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

import alluxio.ConfigurationTestUtils;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link HdfsUnderFileSystemFactory}.
 */
public class HdfsUnderFileSystemFactoryTest {

  /**
   * This test ensures the HDFS UFS module correctly accepts paths that begin with hdfs://.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory =
        UnderFileSystemFactoryRegistry.find("hdfs://localhost/test/path",
            ConfigurationTestUtils.defaults());
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for HDFS paths when using this module", factory);

    factory = UnderFileSystemFactoryRegistry.find("s3://localhost/test/path",
        ConfigurationTestUtils.defaults());
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for S3 paths when using this module", factory);

    factory = UnderFileSystemFactoryRegistry.find("s3n://localhost/test/path",
        ConfigurationTestUtils.defaults());
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for S3 paths when using this module", factory);

    factory = UnderFileSystemFactoryRegistry.find("alluxio://localhost:19999/test",
        ConfigurationTestUtils.defaults());
    Assert.assertNull("A UnderFileSystemFactory should not exist for non supported paths when "
        + "using this module", factory);
  }
}
