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

import alluxio.AlluxioURI;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;

import org.apache.hadoop.conf.Configuration;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A variant of {@link HdfsUnderFileSystem} that instead uses the Gluster FS.
 * <p>
 * Currently this implementation simply manages the extra configuration setup necessary to connect
 * to Gluster FS.
 * </p>
 */
@ThreadSafe
public final class GlusterFSUnderFileSystem extends HdfsUnderFileSystem {

  /**
   * Constant for the Gluster FS URI scheme.
   */
  public static final String SCHEME = "glusterfs://";

  /**
   * Prepares the configuration for this Gluster FS as an HDFS configuration.
   *
   * @param path the path in GlusterFS to serve as the root of this UFS
   * @param cnf the configuration for this UFS
   * @return the created configuration
   */
  public static Configuration createConfiguration(String path,
      UnderFileSystemConfiguration cnf) {
    if (path.startsWith(SCHEME)) {
      Configuration glusterFsConf = new Configuration();
      // Configure for Gluster FS
      glusterFsConf.set("fs.glusterfs.impl", cnf.getValue(PropertyKey.UNDERFS_GLUSTERFS_IMPL));
      glusterFsConf
          .set("mapred.system.dir", cnf.getValue(PropertyKey.UNDERFS_GLUSTERFS_MR_DIR));
      glusterFsConf
          .set("fs.glusterfs.volumes", cnf.getValue(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES));
      glusterFsConf.set(
          "fs.glusterfs.volume.fuse." + cnf.getValue(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES),
          cnf.getValue(PropertyKey.UNDERFS_GLUSTERFS_MOUNTS));
      return glusterFsConf;
    } else {
      // If not Gluster FS fall back to default HDFS behavior
      // This should only happen if someone creates an instance of this directly rather than via the
      // registry and factory which enforces the GlusterFS prefix being present.
      return HdfsUnderFileSystem.createConfiguration(cnf);
    }
  }

  /**
   * Factory method to construct a new Gluster FS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return a new Gluster FS {@link UnderFileSystem} instance
   */
  public static GlusterFSUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    Configuration glusterFsConf = createConfiguration(uri.toString(), conf);
    return new GlusterFSUnderFileSystem(uri, conf, glusterFsConf);
  }

  /**
   * Constructs a new Gluster FS {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param glusterFsConf the configuration for this Gluster FS
   */
  private GlusterFSUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      Configuration glusterFsConf) {
    super(ufsUri, conf, glusterFsConf);
  }

  @Override
  public String getUnderFSType() {
    return "glusterfs";
  }
}
