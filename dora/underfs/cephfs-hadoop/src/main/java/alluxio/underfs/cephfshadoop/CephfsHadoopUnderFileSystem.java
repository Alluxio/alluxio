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

package alluxio.underfs.cephfshadoop;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;

import org.apache.hadoop.conf.Configuration;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Cephfs-Hadoop {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class CephfsHadoopUnderFileSystem extends HdfsUnderFileSystem {

  /**
   * Factory method to constructs a new cephfs-hadoop {@link UnderFileSystem} instance.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop
   * @return a new cephfs-hadoop {@link UnderFileSystem} instance
   */
  public static CephfsHadoopUnderFileSystem createInstance(AlluxioURI ufsUri,
      UnderFileSystemConfiguration conf) {
    Configuration hdfsConf = createConfiguration(conf);
    return new CephfsHadoopUnderFileSystem(ufsUri, conf, hdfsConf);
  }

  /**
   * Constructs a new Cephfs-Hadoop {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param hdfsConf the configuration for Cephfs-Hadoop
   */
  public CephfsHadoopUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      Configuration hdfsConf) {
    super(ufsUri, conf, hdfsConf);
  }

  @Override
  public String getUnderFSType() {
    return "cephfs-hadoop";
  }
}
