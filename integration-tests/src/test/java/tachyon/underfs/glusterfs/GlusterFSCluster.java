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

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystemCluster;

public class GlusterFSCluster extends UnderFileSystemCluster {

  public GlusterFSCluster(String baseDir, TachyonConf tachyonConf) {
    super(baseDir, tachyonConf);
    checkGlusterConfigured(tachyonConf);
  }

  private void checkGlusterConfigured(TachyonConf conf) {
    if (conf == null) {
      throw new NullPointerException("Null Tachyon Configuration provided");
    }
    if (StringUtils.isEmpty(conf.get(Constants.UNDERFS_GLUSTERFS_MOUNTS, null))) {
      throw new IllegalArgumentException("Gluster FS Mounts are undefined");
    }
    if (StringUtils.isEmpty(conf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES, null))) {
      throw new IllegalArgumentException("Gluster FS Volumes are undefined");
    }
  }

  @Override
  public String getUnderFilesystemAddress() {
    checkGlusterConfigured(mTachyonConf);

    return "glusterfs:///tachyon_test";
  }

  @Override
  public boolean isStarted() {
    checkGlusterConfigured(mTachyonConf);
    return true;
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {
    checkGlusterConfigured(mTachyonConf);
  }
}
