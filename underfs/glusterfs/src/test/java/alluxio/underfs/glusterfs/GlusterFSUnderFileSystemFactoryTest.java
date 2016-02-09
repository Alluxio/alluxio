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

package alluxio.underfs.glusterfs;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link GlusterFSUnderFileSystem}.
 */
public class GlusterFSUnderFileSystemFactoryTest {
  private UnderFileSystem mGfs = null;
  private String mMount = null;
  private String mVolume = null;
  private Configuration mConfiguration;

  /**
   * Sets the volume and the mount directory before a test runs.
   */
  @Before
  public final void before() {
    mConfiguration = new Configuration();
    if (mConfiguration.containsKey(Constants.UNDERFS_GLUSTERFS_MR_DIR)) {
      mMount = mConfiguration.get(Constants.UNDERFS_GLUSTERFS_MR_DIR);
    }
    if (mConfiguration.containsKey(Constants.UNDERFS_GLUSTERFS_VOLUMES)) {
      mVolume = mConfiguration.get(Constants.UNDERFS_GLUSTERFS_VOLUMES);
    }
  }

  /**
   * Tests the {@link UnderFileSystem#create(String)} method.
   *
   * @throws Exception when the creation fails
   */
  @Test
  public void createGlusterFS() throws Exception {
    // Using Assume will mark the tests as skipped rather than passed which provides a truer
    // indication of their status
    Assume.assumeTrue(!StringUtils.isEmpty(mMount));
    Assume.assumeTrue(!StringUtils.isEmpty(mVolume));

    mGfs = UnderFileSystem.get("glusterfs:///", mConfiguration);
    Assert.assertNotNull(mGfs.create("alluxio_test"));
  }

  /**
   * Tests the {@link UnderFileSystemRegistry#find(String, Configuration)} method.
   */
  @Test
  public void factoryTest() {
    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("glusterfs://localhost/test/path", mConfiguration);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for Gluster FS paths when using this module",
        factory);

    factory = UnderFileSystemRegistry.find("alluxio://localhost/test/path", mConfiguration);
    Assert.assertNull("A UnderFileSystemFactory should not exist for unsupported paths when using"
        + " this module.", factory);
  }
}
