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

package alluxio.underfs.local;

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link LocalUnderFileSystemFactory}.
 */
public class LocalUnderFileSystemFactoryTest {

  /**
   * This test ensures the local UFS module correctly accepts paths that begin with / or file://.
   */
  @Test
  public void factoryTest() {
    Configuration conf = new Configuration();

    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("/local/test/path", conf);
    UnderFileSystemFactory factory2 = UnderFileSystemRegistry.find("file://local/test/path", conf);
    UnderFileSystemFactory factory3 = UnderFileSystemRegistry.find("hdfs://test-bucket/path", conf);
    UnderFileSystemFactory factory4 = UnderFileSystemRegistry.find("R:\\ramfs\\", conf);
    UnderFileSystemFactory factory5 = UnderFileSystemRegistry.find("file://R:/famfs", conf);
    UnderFileSystemFactory factory6 = UnderFileSystemRegistry.find("R:/ramfs/", conf);

    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory2);
    Assert.assertNull(
        "A UnderFileSystemFactory should not exist for non local paths when using this module",
        factory3);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory4);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory5);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for local paths when using this module", factory6);
  }
}
