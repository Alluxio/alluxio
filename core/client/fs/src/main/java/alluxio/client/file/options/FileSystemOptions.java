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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.base.Preconditions;

/**
 * Options for creating the {@link alluxio.client.file.FileSystem}.
 */
public class FileSystemOptions {
  private FileSystemType mFileSystemType;
  private final boolean mMetadataCacheEnabled;
  private final boolean mDataCacheEnabled;
  private final UfsFileSystemOptions mUfsFileSystemOptions;

  /**
   * Creates the file system options.
   *
   * @param conf alluxio configuration
   * @return the file system options
   */
  public static FileSystemOptions create(AlluxioConfiguration conf) {
    return Builder.create(conf).build();
  }

  /**
   * Creates the default file system options.
   *
   * @return the file system options
   */
  public static FileSystemOptions defaults() {
    return new Builder().build();
  }

  /**
   * Creates a new instance of {@link FileSystemOptions}.
   *
   * @param fileSystemType the underlying file system type
   * @param metadaCacheEnabled whether metadata cache is enabled
   * @param dataCacheEnabled whether data cache is enabled
   * @param ufsFileSystemOptions the ufs file system options
   */
  private FileSystemOptions(FileSystemType fileSystemType,
      boolean metadaCacheEnabled, boolean dataCacheEnabled,
      UfsFileSystemOptions ufsFileSystemOptions) {
    Preconditions.checkState(fileSystemType == FileSystemType.Alluxio
        || (fileSystemType == FileSystemType.Ufs
            && ufsFileSystemOptions.getUfsAddress().isPresent()),
        "ufs address should be set when the base file system is UFS");
    mFileSystemType = fileSystemType;
    mMetadataCacheEnabled = metadaCacheEnabled;
    mDataCacheEnabled = dataCacheEnabled;
    mUfsFileSystemOptions = ufsFileSystemOptions;
  }

  /**
   * @return the underlying file system type
   */
  public FileSystemType getFileSystemType() {
    return mFileSystemType;
  }

  /**
   * @return the ufs file system options;
   */
  public UfsFileSystemOptions getUfsFileSystemOptions() {
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

  /**
   * The type if underlying base file system.
   */
  public enum FileSystemType {
    Alluxio,
    Ufs,
  }

  /**
   * Builder class for {@link FileSystemOptions}.
   */
  public static final class Builder {
    private FileSystemType mFileSystemType = FileSystemType.Alluxio;
    private boolean mMetadataCacheEnabled;
    private boolean mDataCacheEnabled;
    private UfsFileSystemOptions mUfsFileSystemOptions;

    /**
     * Creates a new Builder based on configuration.
     *
     * @param conf the alluxio conf
     * @return the created builder
     */
    public static Builder create(AlluxioConfiguration conf) {
      return new Builder()
          .setMetadataCacheEnabled(conf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED))
          .setDataCacheEnabled(conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_ENABLED));
    }

    /**
     * Constructor.
     */
    public Builder() {}

    /**
     * @param fileSystemType the underlying file system type
     * @return the builder
     */
    public Builder setFileSystemType(FileSystemType fileSystemType) {
      mFileSystemType = fileSystemType;
      return this;
    }

    /**
     * @param ufsFileSystemOptions the ufs file system options
     * @return the builder
     */
    public Builder setUfsFileSystemOptions(UfsFileSystemOptions ufsFileSystemOptions) {
      mUfsFileSystemOptions = ufsFileSystemOptions;
      return this;
    }

    /**
     * @param metadataCacheEnabled metadata cache enabled
     * @return the builder
     */
    public Builder setMetadataCacheEnabled(boolean metadataCacheEnabled) {
      mMetadataCacheEnabled = metadataCacheEnabled;
      return this;
    }

    /**
     * @param dataCacheEnabled data cache enabled
     * @return the builder
     */
    public Builder setDataCacheEnabled(boolean dataCacheEnabled) {
      mDataCacheEnabled = dataCacheEnabled;
      return this;
    }

    /**
     * @return the file system options
     */
    public FileSystemOptions build() {
      return new FileSystemOptions(mFileSystemType, mMetadataCacheEnabled, mDataCacheEnabled,
          mUfsFileSystemOptions == null
              ? new UfsFileSystemOptions.Builder().build() : mUfsFileSystemOptions);
    }
  }
}

