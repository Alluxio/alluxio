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
import alluxio.underfs.UnderFileSystemCluster;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class GlusterFSCluster extends UnderFileSystemCluster {

  public GlusterFSCluster(String baseDir) {
    super(baseDir);
    checkGlusterConfigured();
  }

  private void checkGlusterConfigured() {
    if (StringUtils.isEmpty(Configuration.get(PropertyKey.UNDERFS_GLUSTERFS_MOUNTS))) {
      throw new IllegalArgumentException("Gluster FS Mounts are undefined");
    }
    if (StringUtils.isEmpty(Configuration.get(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES))) {
      throw new IllegalArgumentException("Gluster FS Volumes are undefined");
    }
  }

  @Override
  public String getUnderFilesystemAddress() {
    checkGlusterConfigured();

    return "glusterfs:///alluxio_test";
  }

  @Override
  public boolean isStarted() {
    checkGlusterConfigured();
    return true;
  }

  @Override
  public void cleanup() throws IOException {
    // Not implemented
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {
    checkGlusterConfigured();
  }
}
