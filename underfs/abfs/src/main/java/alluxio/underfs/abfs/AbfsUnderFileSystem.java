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

package alluxio.underfs.abfs;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.options.FileLocationOptions;

import com.google.common.base.MoreObjects;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A Microsoft Azure Data Lake Storage Gen2 Implementation.
 */
@ThreadSafe
public class AbfsUnderFileSystem extends HdfsUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsUnderFileSystem.class);

  private static Configuration createAbfsConfiguration(UnderFileSystemConfiguration conf) {
    Configuration abfsConf = HdfsUnderFileSystem.createConfiguration(conf);

    boolean sharedKey = false;

    for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (PropertyKey.Template.UNDERFS_ABFS_ACCOUNT_KEY.matches(key)) {
        abfsConf.set(key, value);
        final String authTypeKey = key.replace(".key", ".auth.type");
        abfsConf.set(authTypeKey, "SharedKey");
        sharedKey = true;
        break;
      }
    }

    if (!sharedKey) {
      if (conf.isSet(PropertyKey.ABFS_CLIENT_ENDPOINT)
          && conf.isSet(PropertyKey.ABFS_CLIENT_ID)
          && conf.isSet(PropertyKey.ABFS_CLIENT_SECRET)) {
        abfsConf.set("fs.azure.account.auth.type", "OAuth");
        abfsConf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        abfsConf.set(PropertyKey.ABFS_CLIENT_ENDPOINT.getName(),
            conf.get(PropertyKey.ABFS_CLIENT_ENDPOINT));
        abfsConf.set(PropertyKey.ABFS_CLIENT_ID.getName(),
            conf.get(PropertyKey.ABFS_CLIENT_ID));
        abfsConf.set(PropertyKey.ABFS_CLIENT_SECRET.getName(),
            conf.get(PropertyKey.ABFS_CLIENT_SECRET));
      }
    }
    return abfsConf;
  }

  /**
   * Creates a new {@link AbfsUnderFileSystem} instance.
   * @param uri the alluxio uri
   * @param conf the ufs configuration
   * @return A new AbfsUnderFileSystem instance
   */
  public static AbfsUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    Configuration abfsConf = createAbfsConfiguration(conf);
    return new AbfsUnderFileSystem(uri, conf, abfsConf);
  }

  /**
   * Constructs a new HDFS {@link alluxio.underfs.UnderFileSystem}.
   *
   * @param ufsUri   the {@link AlluxioURI} for this UFS
   * @param conf     the configuration for this UFS
   * @param hdfsConf the configuration for HDFS
   */
  public AbfsUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      Configuration hdfsConf) {
    super(ufsUri, conf, hdfsConf);
  }

  @Override
  public String getUnderFSType() {
    return "abfs";
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    // abfs is an object store, so use the default block size, like other object stores.
    return mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    UfsStatus status = super.getStatus(path);
    if (status instanceof UfsFileStatus) {
      // abfs is backed by an object store but always claims its block size to be 512MB.
      // reset the block size in UfsFileStatus according to getBlockSizeByte
      return new UfsFileStatus(path,
          ((UfsFileStatus) status).getContentHash(),
          ((UfsFileStatus) status).getContentLength(),
          MoreObjects.firstNonNull(status.getLastModifiedTime(), 0L),
          status.getOwner(), status.getGroup(), status.getMode(),
          getBlockSizeByte(path));
    }
    return status;
  }

  // No Op
  @Override
  public void setOwner(String path, String user, String group) {
    return;
  }

  // No Op
  @Override
  public void setMode(String path, short mode) {
    return;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.debug("getFileLocations is not supported when using AbfsUnderFileSystem.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    LOG.debug("getFileLocations is not supported when using AbfsUnderFileSystem.");
    return null;
  }
}
