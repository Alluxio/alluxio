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

package alluxio.underfs.orangefs;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystemCluster;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class OrangeFSUnderStorageCluster extends UnderFileSystemCluster {

  public OrangeFSUnderStorageCluster(String baseDir, Configuration configuration) {
    super(baseDir, configuration);
    checkGlusterConfigured(configuration);
  }

  private void checkGlusterConfigured(Configuration conf) {
    if (conf == null) {
      throw new NullPointerException("Null Alluxio Configuration provided");
    }
    if (StringUtils.isEmpty(conf.get(Constants.UNDERFS_OFS_SYSTEMS))) {
      throw new IllegalArgumentException("OrangeFS Systems are undefined");
    }
    if (StringUtils.isEmpty(conf.get(Constants.UNDERFS_OFS_MNTLOCATIONS))) {
      throw new IllegalArgumentException("OrangeFS Mounts are undefined");
    }
  }

  @Override
  public String getUnderFilesystemAddress() {
    checkGlusterConfigured(mConfiguration);

    return "ofs://localhost-orangefs:3334/alluxio_test";
  }

  @Override
  public boolean isStarted() {
    checkGlusterConfigured(mConfiguration);
    return true;
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {
    checkGlusterConfigured(mConfiguration);
  }
}
