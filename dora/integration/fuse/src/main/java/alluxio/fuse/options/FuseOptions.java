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
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.utils.LibfuseVersion;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Options for creating the Fuse filesystem.
 */
public class FuseOptions {
  private static final Logger LOG = LoggerFactory.getLogger(FuseOptions.class);
  public static final PropertyKey FUSE_UPDATE_CHECK_ENABLED =
      PropertyKey.Builder.booleanBuilder("fuse.update.check.enabled")
          .setIsBuiltIn(false)
          .setDefaultValue(false)
          .buildUnregistered();

  /**
   * The UFS root that Fuse mounts.
   * In standalone Fuse SDK, this is different from {@link PropertyKey#DORA_CLIENT_UFS_ROOT}.
   * */
  public static final PropertyKey FUSE_UFS_ROOT =
      PropertyKey.Builder.stringBuilder("fuse.ufs.root")
          .setIsBuiltIn(false)
          .buildUnregistered();

  private final FileSystemOptions mFileSystemOptions;
  private final Set<String> mFuseMountOptions;
  private final boolean mUpdateCheckEnabled;
  private final boolean mSpecialCommandEnabled;

  /**
   * Creates a new instance of {@link FuseOptions}.
   *
   * @param fileSystemOptions the file system options
   * @param fuseMountOptions the FUSE mount options
   * @param updateCheckEnabled whether to enable update check
   * @param specialCommandEnabled whether fuse special commands are enabled
   */
  protected FuseOptions(FileSystemOptions fileSystemOptions,
      Set<String> fuseMountOptions, boolean updateCheckEnabled, boolean specialCommandEnabled) {
    mFileSystemOptions = Preconditions.checkNotNull(fileSystemOptions);
    mFuseMountOptions = Preconditions.checkNotNull(fuseMountOptions);
    mUpdateCheckEnabled = updateCheckEnabled;
    mSpecialCommandEnabled = specialCommandEnabled;
  }

  /**
   * @return the file system options
   */
  public FileSystemOptions getFileSystemOptions() {
    return mFileSystemOptions;
  }

  /**
   * @return the FUSE mount options
   */
  public Set<String> getFuseMountOptions() {
    return mFuseMountOptions;
  }

  /**
   * @return true if update check is enabled
   */
  public boolean updateCheckEnabled() {
    return mUpdateCheckEnabled;
  }

  /**
   * @return true if fuse special command is enabled
   */
  public boolean specialCommandEnabled() {
    return mSpecialCommandEnabled;
  }

  /**
   * Builder for Fuse options.
   */
  public static class Builder {
    private FileSystemOptions mFileSystemOptions;
    private Set<String> mFuseMountOptions;
    private boolean mUpdateCheckEnabled;
    private boolean mSpecialCommandEnabled;

    /**
     * Creates a new builder with default settings.
     */
    public Builder() { }

    /**
     * Creates a new builder pre-populated with settings from the given configurations.
     *
     * @param conf configurations
     * @return builder
     */
    public static Builder fromConfig(AlluxioConfiguration conf) {
      FuseOptions.Builder builder = new FuseOptions.Builder();

      // Set update check
      final boolean updateCheckEnabled;
      if (!conf.isSetByUser(FUSE_UPDATE_CHECK_ENABLED)) {
        // Standalone FUSE SDK without dora distributed cache
        updateCheckEnabled = !conf.getBoolean(PropertyKey.DORA_ENABLED)
            && conf.isSetByUser(PropertyKey.DORA_CLIENT_UFS_ROOT);
      } else {
        updateCheckEnabled = conf.getBoolean(FUSE_UPDATE_CHECK_ENABLED);
      }
      builder.setUpdateCheckEnabled(updateCheckEnabled);

      // Set mount options
      HashSet<String> mountOptions = new HashSet<>(conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS));
      LibfuseVersion version = AlluxioFuseUtils.getLibfuseVersion(conf);
      if (version == LibfuseVersion.VERSION_2) {
        // Without option big_write, the kernel limits a single writing request to 4k.
        // With option big_write, maximum of a single writing request is 128k.
        // See https://github.com/libfuse/libfuse/blob/fuse_2_9_3/ChangeLog#L655-L659,
        // and https://github.com/torvalds/linux/commit/78bb6cb9a890d3d50ca3b02fce9223d3e734ab9b.
        // Libfuse3 dropped this option because it's default
        String bigWritesOptions = "big_writes";
        if (mountOptions.add(bigWritesOptions)) {
          LOG.info("Added fuse mount option {} to enlarge single write request size",
              bigWritesOptions);
        }
      } else {
        if (mountOptions.remove("direct_io")) {
          // TODO(lu) implement direct_io with libfuse3
          LOG.error("FUSE 3 does not support direct_io mount option");
        }
        if (mountOptions.stream().noneMatch(a -> a.startsWith("max_idle_threads"))) {
          String idleThreadsOption = "max_idle_threads=64";
          mountOptions.add(idleThreadsOption);
          LOG.info("Added fuse mount option {} for FUSE 3", idleThreadsOption);
        }
      }
      builder.setFuseMountOptions(mountOptions);

      // Set special commands
      boolean specialCommandsEnabled = conf.getBoolean(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED);
      builder.setSpecialCommandEnabled(specialCommandsEnabled);

      // Set UFS options
      final FileSystemOptions fileSystemOptions;
      if (conf.isSetByUser(FuseOptions.FUSE_UFS_ROOT)) {
        // override UFS with fuse's own property, coming from the command line user input
        UfsFileSystemOptions ufsFileSystemOptions =
            new UfsFileSystemOptions(conf.getString(FuseOptions.FUSE_UFS_ROOT));
        fileSystemOptions = FileSystemOptions.Builder.fromConf(conf)
            .setUfsFileSystemOptions(ufsFileSystemOptions)
            .build();
      } else {
        fileSystemOptions = FileSystemOptions.Builder.fromConf(conf).build();
      }
      builder.setFileSystemOptions(fileSystemOptions);

      return builder;
    }

    /**
     * @return file system options
     */
    public FileSystemOptions getFileSystemOptions() {
      return mFileSystemOptions;
    }

    /**
     * Set file system options.
     *
     * @param fileSystemOptions
     * @return this builder
     */
    public Builder setFileSystemOptions(FileSystemOptions fileSystemOptions) {
      mFileSystemOptions = fileSystemOptions;
      return this;
    }

    /**
     * @return mount options
     */
    public Set<String> getFuseMountOptions() {
      return mFuseMountOptions;
    }

    /**
     * Sets mount options.
     *
     * @param fuseMountOptions
     * @return this builder
     */
    public Builder setFuseMountOptions(Set<String> fuseMountOptions) {
      mFuseMountOptions = fuseMountOptions;
      return this;
    }

    /**
     * @return if update check is enabled
     */
    public boolean isUpdateCheckEnabled() {
      return mUpdateCheckEnabled;
    }

    /**
     * Enables or disables update check.
     *
     * @param updateCheckEnabled
     * @return this builder
     */
    public Builder setUpdateCheckEnabled(boolean updateCheckEnabled) {
      mUpdateCheckEnabled = updateCheckEnabled;
      return this;
    }

    /**
     * @return if fuse special commands are enabled
     */
    public boolean isSpecialCommandEnabled() {
      return mSpecialCommandEnabled;
    }

    /**
     * Enables or disabled fuse special commands.
     *
     * @param specialCommandEnabled
     * @return this builder
     */
    public Builder setSpecialCommandEnabled(boolean specialCommandEnabled) {
      mSpecialCommandEnabled = specialCommandEnabled;
      return this;
    }

    /**
     * Builds Fuse options.
     *
     * @return fuse options
     */
    public FuseOptions build() {
      return new FuseOptions(mFileSystemOptions, mFuseMountOptions, mUpdateCheckEnabled,
          mSpecialCommandEnabled);
    }
  }
}
