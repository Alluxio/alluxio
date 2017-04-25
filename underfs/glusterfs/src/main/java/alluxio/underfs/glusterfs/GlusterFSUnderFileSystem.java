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
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.util.UnderFileSystemUtils;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

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
   * Prepares the Hadoop configuration necessary.
   *
   * @param ufsConf Hadoop configuration
   */
  public static Configuration prepareConfiguration(String path, Map<String, String> ufsConf) {
    if (path.startsWith(SCHEME)) {
      Configuration hadoopConf = new Configuration();
      // Configure for Gluster FS
      hadoopConf.set("fs.glusterfs.impl",
          UnderFileSystemUtils.getValue(PropertyKey.UNDERFS_GLUSTERFS_IMPL, ufsConf));
      hadoopConf.set("mapred.system.dir",
          UnderFileSystemUtils.getValue(PropertyKey.UNDERFS_GLUSTERFS_MR_DIR, ufsConf));
      hadoopConf.set("fs.glusterfs.volumes",
          UnderFileSystemUtils.getValue(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES, ufsConf));
      hadoopConf.set("fs.glusterfs.volume.fuse."
          + UnderFileSystemUtils.getValue(PropertyKey.UNDERFS_GLUSTERFS_VOLUMES, ufsConf),
          UnderFileSystemUtils.getValue(PropertyKey.UNDERFS_GLUSTERFS_MOUNTS, ufsConf));
      return hadoopConf;
    } else {
      // If not Gluster FS fall back to default HDFS behavior
      // This should only happen if someone creates an instance of this directly rather than via the
      // registry and factory which enforces the GlusterFS prefix being present.
      return HdfsUnderFileSystem.createConfiguration(ufsConf);
    }
  }

  /**
   * Factory method to construct a new Gluster FS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop or GlusterFS
   */
  public static GlusterFSUnderFileSystem createInstance(AlluxioURI uri, Map<String, String> conf) {
    Configuration hadoopConf = prepareConfiguration(uri.toString(), conf);
    return new GlusterFSUnderFileSystem(uri, conf, hadoopConf);
  }

  /**
   * Constructs a new Gluster FS {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param ufsConf the configuration for ufs
   * @param hadoopConf the configuration for hdfs
   */
  private GlusterFSUnderFileSystem(AlluxioURI ufsUri, Map<String, String> ufsConf, Configuration
      hadoopConf)  {
    super(ufsUri, ufsConf, hadoopConf);
  }

  @Override
  public String getUnderFSType() {
    return "glusterfs";
  }
}
