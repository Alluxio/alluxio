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

package alluxio.underfs.glusterfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A variant of {@link HdfsUnderFileSystem} that instead uses the Gluster FS.
 * <p>
 * Currently this implementation simply manages the extra configuration setup necessary to connect
 * to Gluster FS.
 * </p>
 */
@ThreadSafe
public class GlusterFSUnderFileSystem extends HdfsUnderFileSystem {

  /**
   * Constant for the Gluster FS URI scheme.
   */
  public static final String SCHEME = "glusterfs://";

  /**
   * Constructs a new Gluster FS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param configuration the configuration for Alluxio
   * @param conf the configuration for Hadoop or GlusterFS
   */
  public GlusterFSUnderFileSystem(AlluxioURI uri, Configuration configuration, Object conf) {
    super(uri, configuration, conf);
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.GLUSTERFS;
  }

  @Override
  protected void prepareConfiguration(String path, Configuration conf,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    if (path.startsWith(SCHEME)) {
      // Configure for Gluster FS
      hadoopConf.set("fs.glusterfs.impl", conf.get(Constants.UNDERFS_GLUSTERFS_IMPL));
      hadoopConf.set("mapred.system.dir",
          conf.get(Constants.UNDERFS_GLUSTERFS_MR_DIR));
      hadoopConf
          .set("fs.glusterfs.volumes", conf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES));
      hadoopConf.set(
          "fs.glusterfs.volume.fuse." + conf.get(Constants.UNDERFS_GLUSTERFS_VOLUMES),
          conf.get(Constants.UNDERFS_GLUSTERFS_MOUNTS));
    } else {
      // If not Gluster FS fall back to default HDFS behaviour
      // This should only happen if someone creates an instance of this directly rather than via the
      // registry and factory which enforces the GlusterFS prefix being present.
      super.prepareConfiguration(path, conf, hadoopConf);
    }
  }

}
