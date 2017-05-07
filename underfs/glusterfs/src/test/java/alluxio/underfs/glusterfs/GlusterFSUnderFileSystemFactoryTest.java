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

package alluxio.underfs.glusterfs;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link GlusterFSUnderFileSystem}.
 */
public class GlusterFSUnderFileSystemFactoryTest {
  private String mMount = null;
  private String mVolume = null;

  /**
   * Sets the volume and the mount directory before a test runs.
   */
  @Before
  public final void before() {
    if (Configuration.containsKey(PropertyKey.UNDERFS_GLUSTERFS_MR_DIR)) {
      mMount = Configuration.get(PropertyKey.UNDERFS_GLUSTERFS_MR_DIR);
    }
    if (Configuration.containsKey(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES)) {
      mVolume = Configuration.get(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES);
    }
  }

  /**
   * Tests the {@link UnderFileSystem#create(String)} method.
   */
  @Test
  public void createGlusterFS() throws Exception {
    // Using Assume will mark the tests as skipped rather than passed which provides a truer
    // indication of their status
    Assume.assumeTrue(!StringUtils.isEmpty(mMount));
    Assume.assumeTrue(!StringUtils.isEmpty(mVolume));

    UnderFileSystem gfs = UnderFileSystem.Factory.create("glusterfs:///");
    Assert.assertNotNull(gfs.create("alluxio_test"));
  }

  /**
   * Tests the {@link UnderFileSystemFactoryRegistry#find(String)} method.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory =
        UnderFileSystemFactoryRegistry.find("glusterfs://localhost/test/path");
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for Gluster FS paths when using this module",
        factory);

    factory = UnderFileSystemFactoryRegistry.find("alluxio://localhost/test/path");
    Assert.assertNull("A UnderFileSystemFactory should not exist for unsupported paths when using"
        + " this module.", factory);
  }
}
