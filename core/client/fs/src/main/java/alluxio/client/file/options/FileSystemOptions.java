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

package alluxio.client.file.options;

import alluxio.client.file.FileSystemUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.base.Preconditions;

import java.util.Optional;

/**
 * Options for creating the {@link alluxio.client.file.FileSystem}.
 */
public class FileSystemOptions {
  private final boolean mMetadataCacheEnabled;
  private final boolean mDataCacheEnabled;
  private final Optional<UfsFileSystemOptions> mUfsFileSystemOptions;

  /**
   * Creates the file system options.
   *
   * @param conf alluxio configuration
   * @return the file system options
   */
  public static FileSystemOptions create(AlluxioConfiguration conf) {
    return create(conf, Optional.empty());
  }

  /**
   * Creates the file system options.
   *
   * @param conf alluxio configuration
   * @param ufsOptions the options for ufs base file system
   * @return the file system options
   */
  public static FileSystemOptions create(AlluxioConfiguration conf,
      Optional<UfsFileSystemOptions> ufsOptions) {
    return new FileSystemOptions(FileSystemUtils.metadataEnabled(conf),
        conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_ENABLED),
        ufsOptions);
  }

  /**
   * Creates a new instance of {@link FileSystemOptions}.
   *
   * @param metadataCacheEnabled whether metadata cache is enabled
   * @param dataCacheEnabled whether data cache is enabled
   * @param ufsFileSystemOptions the ufs file system options
   */
  private FileSystemOptions(boolean metadataCacheEnabled, boolean dataCacheEnabled,
      Optional<UfsFileSystemOptions> ufsFileSystemOptions) {
    mUfsFileSystemOptions = Preconditions.checkNotNull(ufsFileSystemOptions);
    mMetadataCacheEnabled = metadataCacheEnabled;
    mDataCacheEnabled = dataCacheEnabled;
  }

  /**
   * @return the ufs file system options;
   */
  public Optional<UfsFileSystemOptions> getUfsFileSystemOptions() {
    return mUfsFileSystemOptions;
  }

  /**
   * @return true if metadata cache is enabled, false otherwise
   */
  public boolean isMetadataCacheEnabled() {
    return mMetadataCacheEnabled;
  }

  /**
   * @return true if data cache is enabled, false otherwise
   */
  public boolean isDataCacheEnabled() {
    return mDataCacheEnabled;
  }
}

