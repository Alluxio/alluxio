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

package alluxio.underfs.ozone;

import alluxio.AlluxioURI;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.options.CreateOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Ozone {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class OzoneUnderFileSystem extends HdfsUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(OzoneUnderFileSystem.class);

  /**
   * Factory method to constructs a new HDFS {@link UnderFileSystem} instance.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop
   * @return a new HDFS {@link UnderFileSystem} instance
   */
  public static OzoneUnderFileSystem createInstance(AlluxioURI ufsUri,
      UnderFileSystemConfiguration conf) {
    Configuration hdfsConf = createConfiguration(conf);
    return new OzoneUnderFileSystem(ufsUri, conf, hdfsConf);
  }

  /**
   * Constructs a new Ozone {@link UnderFileSystem}.
   *  @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param hdfsConf the configuration for HDFS
   */
  public OzoneUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      Configuration hdfsConf) {
    super(ufsUri, conf, hdfsConf);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    IOException te = null;
    FileSystem hdfs = getFs();
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attempt()) {
      try {
        // TODO(chaomin): support creating HDFS files with specified block size and replication.
        OutputStream outputStream = new OzoneUnderFileOutputStream(
            FileSystem.create(hdfs, new Path(path),
              new FsPermission(options.getMode().toShort())));
        if (options.getAcl() != null) {
          setAclEntries(path, options.getAcl().getEntries());
        }
        return outputStream;
      } catch (IOException e) {
        LOG.warn("Attempt count {} : {} ", retryPolicy.getAttemptCount(), e.toString());
        te = e;
      }
    }
    throw te;
  }
}
