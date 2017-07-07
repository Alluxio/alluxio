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

import alluxio.PropertyKey;
import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import org.apache.commons.lang3.StringUtils;

/**
 * This UFS contract test will use GlusterFS as the backing store.
 */
public final class GlusterFSUnderFileSystemContractTest
    extends AbstractUnderFileSystemContractTest {
  private static final String GLUSTER_FS_BASE_DIR_CONF = "testGlusterFSBaseDir";
  private static final String GLUSTER_FS_BASE_DIR = System.getProperty(GLUSTER_FS_BASE_DIR_CONF);

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    if (StringUtils.isEmpty(conf.getValue(PropertyKey.UNDERFS_GLUSTERFS_MOUNTS))) {
      throw new IllegalArgumentException("Gluster FS Mounts are undefined");
    }
    if (StringUtils.isEmpty(conf.getValue(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES))) {
      throw new IllegalArgumentException("Gluster FS Volumes are undefined");
    }
    return new GlusterFSUnderFileSystemFactory().create(path, conf);
  }

  @Override
  public String getUfsBaseDir() {
    return GLUSTER_FS_BASE_DIR;
  }
}
