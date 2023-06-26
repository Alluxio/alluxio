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

import java.util.Objects;
import java.util.Optional;

/**
 * Options for creating the {@link alluxio.client.file.FileSystem}.
 */
public class FileSystemOptions {
  private final boolean mMetadataCacheEnabled;
  private final boolean mDataCacheEnabled;
  private final boolean mDoraCacheEnabled;
  private final boolean mUfsFallbackEnabled;
  private final Optional<UfsFileSystemOptions> mUfsFileSystemOptions;

  /**
   * Builder for {@link FileSystemOptions}.
   */
  public static class Builder {
    private boolean mMetadataCacheEnabled;
    private boolean mDataCacheEnabled;
    private boolean mDoraCacheEnabled;
    private boolean mUfsFallbackEnabled;
    private Optional<UfsFileSystemOptions> mUfsFileSystemOptions = Optional.empty();

    /**
     * Creates new builder from configuration.
     *
     * @param conf configuration
     * @return new builder with options set to values from configuration
     */
    public static Builder fromConf(AlluxioConfiguration conf) {
      Builder builder = new Builder();
      builder.setDataCacheEnabled(conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_ENABLED))
          .setMetadataCacheEnabled(FileSystemUtils.metadataEnabled(conf))
          .setDoraCacheEnabled(
              conf.getBoolean(PropertyKey.DORA_ENABLED))
          .setUfsFallbackEnabled(conf.getBoolean(PropertyKey.DORA_CLIENT_UFS_FALLBACK_ENABLED));
      //TODO(bowen): ufs root is required temporarily even though ufs fall back is disabled
      builder.setUfsFileSystemOptions(
          new UfsFileSystemOptions(conf.getString(PropertyKey.DORA_CLIENT_UFS_ROOT)));
      return builder;
    }

    /**
     * @return whether client metadata cache is enabled
     */
    public boolean isMetadataCacheEnabled() {
      return mMetadataCacheEnabled;
    }

    /**
     * @param metadataCacheEnabled whether client metadata cache is enabled
     * @return this
     */
    public Builder setMetadataCacheEnabled(boolean metadataCacheEnabled) {
      mMetadataCacheEnabled = metadataCacheEnabled;
      return this;
    }

    /**
     * @return whether client local data cache is enabled
     */
    public boolean isDataCacheEnabled() {
      return mDataCacheEnabled;
    }

    /**
     * @param dataCacheEnabled whether client local data cache is enabled
     * @return this
     */
    public Builder setDataCacheEnabled(boolean dataCacheEnabled) {
      mDataCacheEnabled = dataCacheEnabled;
      return this;
    }

    /**
     * @return whether dora worker cache is enabled
     */
    public boolean isDoraCacheEnabled() {
      return mDoraCacheEnabled;
    }

    /**
     * @param doraCacheEnabled whether dora worker cache is enabled
     * @return this
     */
    public Builder setDoraCacheEnabled(boolean doraCacheEnabled) {
      mDoraCacheEnabled = doraCacheEnabled;
      return this;
    }

    /**
     * @return Whether UFS fallback is enabled
     */
    public boolean isUfsFallbackEnabled() {
      return mUfsFallbackEnabled;
    }

    /**
     * @param ufsFallbackEnabled whether UFS fallback is enabled
     * @return this
     */
    public Builder setUfsFallbackEnabled(boolean ufsFallbackEnabled) {
      mUfsFallbackEnabled = ufsFallbackEnabled;
      return this;
    }

    /**
     * @return UFS file system options
     */
    public Optional<UfsFileSystemOptions> getUfsFileSystemOptions() {
      return mUfsFileSystemOptions;
    }

    /**
     * @param ufsFileSystemOptions UFS file system options
     * @return this
     */
    public Builder setUfsFileSystemOptions(UfsFileSystemOptions ufsFileSystemOptions) {
      mUfsFileSystemOptions = Optional.of(Objects.requireNonNull(ufsFileSystemOptions));
      return this;
    }

    /**
     * @return a new {@link FileSystemOptions} object
     */
    public FileSystemOptions build() {
      return new FileSystemOptions(
          mMetadataCacheEnabled,
          mDataCacheEnabled,
          mDoraCacheEnabled,
          mUfsFallbackEnabled,
          mUfsFileSystemOptions);
    }
  }

  /**
   * Creates a new instance of {@link FileSystemOptions}.
   *
   * @param metadataCacheEnabled whether metadata cache is enabled
   * @param dataCacheEnabled whether data cache is enabled
   * @param doraCacheEnabled whether dora cache is enabled
   * @param ufsFileSystemOptions the ufs file system options
   */
  private FileSystemOptions(boolean metadataCacheEnabled,
      boolean dataCacheEnabled,
      boolean doraCacheEnabled,
      boolean ufsFallbackEnabled,
      Optional<UfsFileSystemOptions> ufsFileSystemOptions) {
    mUfsFileSystemOptions = Preconditions.checkNotNull(ufsFileSystemOptions);
    mMetadataCacheEnabled = metadataCacheEnabled;
    mDataCacheEnabled = dataCacheEnabled;
    mDoraCacheEnabled = doraCacheEnabled;
    mUfsFallbackEnabled = ufsFallbackEnabled;
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

  /**
   * @return true if dora cache is enabled, false otherwise
   */
  public boolean isDoraCacheEnabled() {
    return mDoraCacheEnabled;
  }

  /**
   * @return whether client UFS fallback is enabled
   */
  public boolean isUfsFallbackEnabled() {
    return mUfsFallbackEnabled;
  }
}

