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

import org.apache.hadoop.conf.Configuration;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.hdfs.HdfsUnderFileSystem;

/**
 * A variant of {@link HdfsUnderFileSystem} that instead uses the Gluster FS.
 * <p>
 * Currently this implementation simply manages the extra configuration setup necessary to connect
 * to Gluster FS.
 */
public class GlusterFSUnderFileSystem extends HdfsUnderFileSystem {

  /**
   * Constant for the Gluster FS URI scheme.
   */
  public static final String SCHEME = "glusterfs://";

  /**
   * Constructs a new Gluster FS {@link UnderFileSystem}.
   *
   * @param fsDefaultName the under FS prefix
   * @param tachyonConf the configuration for Tachyon
   * @param conf the configuration for Hadoop or GlusterFS
   */
  public GlusterFSUnderFileSystem(String fsDefaultName, TachyonConf tachyonConf, Object conf) {
    super(fsDefaultName, tachyonConf, conf);
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.GLUSTERFS;
  }

  @Override
  protected void prepareConfiguration(String path, TachyonConf tachyonConf, Configuration config) {
    if (path.startsWith(SCHEME)) {
      // Configure for Gluster FS
      config.set("fs.glusterfs.impl", tachyonConf.get(Constants.UNDERFS_GLUSTERFS_IMPL));
      config.set("mapred.system.dir",
          tachyonConf.get(Constants.UNDERFS_GLUSTERFS_MR_DIR));
      config
          .set("fs.glusterfs.volumes", tachyonConf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES));
      config.set(
          "fs.glusterfs.volume.fuse." + tachyonConf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES),
          tachyonConf.get(Constants.UNDERFS_GLUSTERFS_MOUNTS));
    } else {
      // If not Gluster FS fall back to default HDFS behaviour
      // This should only happen if someone creates an instance of this directly rather than via the
      // registry and factory which enforces the GlusterFS prefix being present.
      super.prepareConfiguration(path, tachyonConf, config);
    }
  }

}
