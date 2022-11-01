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

package alluxio.fuse.options;

import alluxio.client.file.options.FileSystemOptions;
import alluxio.conf.AlluxioConfiguration;

import com.google.common.base.Preconditions;

/**
 * Options for creating the Fuse filesystem.
 */
public class FuseOptions {
  private final FileSystemOptions mFileSystemOptions;

  /**
   * Creates the FUSE options.
   *
   * @param conf alluxio configuration
   * @return the file system options
   */
  public static FuseOptions create(AlluxioConfiguration conf) {
    return new Builder().setFileSystemOptions(FileSystemOptions.create(conf)).build();
  }

  /**
   * Creates the default file system options.
   *
   * @return the file system options
   */
  public static FuseOptions defaults() {
    return new Builder().build();
  }

  /**
   * Creates a new instance of {@link FuseOptions}.
   *
   * @param fileSystemOptions the file system options
   */
  private FuseOptions(FileSystemOptions fileSystemOptions) {
    mFileSystemOptions = Preconditions.checkNotNull(fileSystemOptions);
  }

  /**
   * @return the file system options
   */
  public FileSystemOptions getFileSystemOptions() {
    return mFileSystemOptions;
  }

  /**
   * Builder class for {@link FuseOptions}.
   */
  public static final class Builder {
    private FileSystemOptions mFileSystemOptions;

    /**
     * Constructor.
     */
    public Builder() {}

    /**
     * @param fileSystemOptions the file system options
     * @return the builder
     */
    public Builder setFileSystemOptions(FileSystemOptions fileSystemOptions) {
      mFileSystemOptions = fileSystemOptions;
      return this;
    }

    /**
     * @return the worker net address
     */
    public FuseOptions build() {
      return new FuseOptions(mFileSystemOptions == null
          ? FileSystemOptions.defaults() : mFileSystemOptions);
    }
  }
}

