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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.util.ConfigurationUtils;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link HdfsUnderFileSystem}.
 *
 * It caches created {@link HdfsUnderFileSystem}s, using the scheme and authority pair as the key.
 */
@ThreadSafe
public class HdfsUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link HdfsUnderFileSystemFactory}.
   */
  public HdfsUnderFileSystemFactory() { }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    return HdfsUnderFileSystem.createInstance(new AlluxioURI(path), conf);
  }

  @Override
  public boolean supportsPath(String path) {
    // This loads the static configuration from the JVM's system properties and the site properties
    // file at the time of instantiation. Because of this, setting the property
    // UNDERFS_HDFS_PREFIXES programmatically *not* work.
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    if (path != null) {
      // TODO(hy): In Hadoop 2.x this can be replaced with the simpler call to
      // FileSystem.getFileSystemClass() without any need for having users explicitly declare the
      // file system schemes to treat as being HDFS. However as long as pre 2.x versions of Hadoop
      // are supported this is not an option and we have to continue to use this method.
      for (final String prefix : conf.getList(PropertyKey.UNDERFS_HDFS_PREFIXES, ",")) {
        if (path.startsWith(prefix)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean supportsPath(String path, UnderFileSystemConfiguration conf) {
    if (path != null) {
      // This loads the configuration from the JVM's system properties and the site properties file
      // on disk. Because of this, setting the property UNDERFS_HDFS_PREFIXES programmatically *not*
      // work.
      AlluxioConfiguration alluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());

      // TODO(hy): In Hadoop 2.x this can be replaced with the simpler call to
      // FileSystem.getFileSystemClass() without any need for having users explicitly declare the
      // file system schemes to treat as being HDFS. However as long as pre 2.x versions of Hadoop
      // are supported this is not an option and we have to continue to use this method.
      for (final String prefix : alluxioConf.getList(PropertyKey.UNDERFS_HDFS_PREFIXES, ",")) {
        if (path.startsWith(prefix)) {
          if (!conf.isSet(PropertyKey.UNDERFS_VERSION)
              || HdfsVersion.matches(conf.get(PropertyKey.UNDERFS_VERSION), getVersion())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public String getVersion() {
    return alluxio.UfsConstants.UFS_HADOOP_VERSION;
  }
}
