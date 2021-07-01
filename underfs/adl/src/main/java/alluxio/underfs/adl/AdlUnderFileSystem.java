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

package alluxio.underfs.adl;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.options.FileLocationOptions;

import com.google.common.base.MoreObjects;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A Microsoft Azure Data Lake Storage Gen 1 Implementation.
 */
@ThreadSafe
public class AdlUnderFileSystem extends HdfsUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AdlUnderFileSystem.class);

  /**
   * Prepares the configuration for this Adl as an HDFS configuration.
   *
   * @param conf the configuration for this UFS
   * @return the created configuration
   */
  public static Configuration createConfiguration(UnderFileSystemConfiguration conf) {
    Configuration adlConf = HdfsUnderFileSystem.createConfiguration(conf);
    for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (PropertyKey.Template.UNDERFS_AZURE_CLIENT_ID.matches(key)) {
        adlConf.set(key, value);
      }
      if (PropertyKey.Template.UNDERFS_AZURE_CLIENT_SECRET.matches(key)) {
        adlConf.set(key, value);
      }
      if (PropertyKey.Template.UNDERFS_AZURE_REFRESH_URL.matches(key)) {
        adlConf.set(key, value);
      }
    }
    LOG.info(adlConf.toString());
    adlConf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential");
    return adlConf;
  }

  /**
   * Factory method to construct a new Adl {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return a new Adl {@link UnderFileSystem} instance
   */
  public static AdlUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    Configuration adlConf = createConfiguration(conf);
    return new AdlUnderFileSystem(uri, conf, adlConf);
  }

  /**
   * Constructs a new Adl {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @param adlConf the configuration for this Adl UFS
   */
  public AdlUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf,
      final Configuration adlConf) {
    super(ufsUri, conf, adlConf);
  }

  @Override
  public String getUnderFSType() {
    return "adl";
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    // adl is an object store, so use the default block size, like other object stores.
    return mUfsConf.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    UfsStatus status = super.getStatus(path);
    if (status instanceof UfsFileStatus) {
      // adl is backed by an object store but always claims its block size to be 512MB.
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
    LOG.debug("getFileLocations is not supported when using AdlUnderFileSystem.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    LOG.debug("getFileLocations is not supported when using AdlUnderFileSystem.");
    return null;
  }
}
