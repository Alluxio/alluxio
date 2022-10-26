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

/**
 * Options for creating the {@link alluxio.client.file.FileSystem}.
 */
public class FileSystemOptions {
  private final boolean mMetadataCacheEnabled;
  private final boolean mDataCacheEnabled;

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
   * @param metadaCacheEnabled whether metadata cache is enabled
   * @param dataCacheEnabled whether data cache is enabled
   */
  private FileSystemOptions(boolean metadaCacheEnabled, boolean dataCacheEnabled) {
    mMetadataCacheEnabled = metadaCacheEnabled;
    mDataCacheEnabled = dataCacheEnabled;
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
   * Builder class for {@link FileSystemOptions}.
   */
  public static final class Builder {
    private boolean mMetadataCacheEnabled;
    private boolean mDataCacheEnabled;

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
     * @return the worker net address
     */
    public FileSystemOptions build() {
      return new FileSystemOptions(mMetadataCacheEnabled, mDataCacheEnabled);
    }
  }
}

