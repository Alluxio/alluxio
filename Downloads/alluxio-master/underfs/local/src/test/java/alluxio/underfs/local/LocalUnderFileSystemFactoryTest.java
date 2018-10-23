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

package alluxio.underfs.local;

import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

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
  public void factory() {
    UnderFileSystemFactory factory = UnderFileSystemFactoryRegistry.find("/local/test/path");
    UnderFileSystemFactory factory2 = UnderFileSystemFactoryRegistry.find("file://local/test/path");
    UnderFileSystemFactory factory3 =
        UnderFileSystemFactoryRegistry.find("hdfs://test-bucket/path");
    UnderFileSystemFactory factory4 = UnderFileSystemFactoryRegistry.find("R:\\ramfs\\");
    UnderFileSystemFactory factory5 = UnderFileSystemFactoryRegistry.find("file://R:/famfs");
    UnderFileSystemFactory factory6 = UnderFileSystemFactoryRegistry.find("R:/ramfs/");

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
