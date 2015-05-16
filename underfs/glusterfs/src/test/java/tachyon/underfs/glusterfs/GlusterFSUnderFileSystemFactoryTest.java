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

package tachyon.underfs.glusterfs;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemFactory;
import tachyon.underfs.UnderFileSystemRegistry;

/**
 * Unit tests for {@code GlusterFSUnderFileSystem}
 */
public class GlusterFSUnderFileSystemFactoryTest {
  private UnderFileSystem mGfs = null;
  private String mMount = null;
  private String mVolume = null;
  private TachyonConf mTachyonConf;

  @Before
  public final void before() throws IOException {
    mTachyonConf = new TachyonConf();
    mMount =  mTachyonConf.get(Constants.UNDERFS_GLUSTERFS_MR_DIR,
        "glusterfs:///mapred/system");
    mVolume = mTachyonConf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES, null);
  }

  @Test
  public void createGlusterFS() throws Exception {
    // Using Assume will mark the tests as skipped rather than passed which provides a truer
    // indication of their status
    Assume.assumeTrue(!StringUtils.isEmpty(mMount));
    Assume.assumeTrue(!StringUtils.isEmpty(mVolume));

    mGfs = UnderFileSystem.get("glusterfs:///", mTachyonConf);
    Assert.assertNotNull(mGfs.create("tachyon_test"));
  }

  @Test
  public void factoryTest() {
    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("glusterfs://localhost/test/path", mTachyonConf);
    Assert.assertNotNull(
        "A UnderFileSystemFactory should exist for Gluster FS paths when using this module",
        factory);

    factory = UnderFileSystemRegistry.find("tachyon://localhost/test/path", mTachyonConf);
    Assert.assertNull("A UnderFileSystemFactory should not exist for unsupported paths when using"
        + " this module.", factory);
  }
}
