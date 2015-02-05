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

import tachyon.conf.CommonConf;
import tachyon.underfs.UnderFileSystemCluster;

public class GlusterFSCluster extends UnderFileSystemCluster {

  public GlusterFSCluster(String baseDir) {
    super(baseDir);
    checkGlusterConfigured();
  }

  private void checkGlusterConfigured() {
    if (StringUtils.isEmpty(CommonConf.get().UNDERFS_GLUSTERFS_MOUNTS)) {
      throw new IllegalArgumentException("Gluster FS Mounts are undefined");
    }
    if (StringUtils.isEmpty(CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES)) {
      throw new IllegalArgumentException("Gluster FS Volumes are undefined");
    }
  }

  @Override
  public String getUnderFilesystemAddress() {
    checkGlusterConfigured();

    return "glusterfs:///tachyon_test";
  }

  @Override
  public boolean isStarted() {
    checkGlusterConfigured();
    return true;
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {
    checkGlusterConfigured();
  }
}
