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
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.utils.LibfuseVersion;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Options for creating the Fuse filesystem.
 */
public class FuseOptions {
  private static final Logger LOG = LoggerFactory.getLogger(FuseOptions.class);
  private final FileSystemOptions mFileSystemOptions;
  private final boolean mUpdateCheckEnabled;
  private final Set<String> mFuseMountOptions;

  /**
   * Creates the FUSE options.
   *
   * @param conf alluxio configuration
   * @param updateCheckEnabled whether to enable update check
   * @return the file system options
   */
  public static FuseOptions create(AlluxioConfiguration conf, boolean updateCheckEnabled) {
    return create(conf, FileSystemOptions.create(conf), updateCheckEnabled);
  }

  /**
   * Creates the FUSE options.
   *
   * @param conf alluxio configuration
   * @param options the file system options
   * @param updateCheckEnabled whether to enable update check
   * @return the file system options
   */
  public static FuseOptions create(AlluxioConfiguration conf,
      FileSystemOptions options, boolean updateCheckEnabled) {
    return new FuseOptions(options, constructFuseMountOptions(conf), updateCheckEnabled);
  }

  private static Set<String> constructFuseMountOptions(AlluxioConfiguration conf) {
    Set<String> options = conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS)
        .stream().filter(a -> !a.isEmpty()).collect(Collectors.toSet());
    if (AlluxioFuseUtils.getLibfuseVersion(conf) == LibfuseVersion.VERSION_2) {
      // Without option big_write, the kernel limits a single writing request to 4k.
      // With option big_write, maximum of a single writing request is 128k.
      // See https://github.com/libfuse/libfuse/blob/fuse_2_9_3/ChangeLog#L655-L659,
      // and https://github.com/torvalds/linux/commit/78bb6cb9a890d3d50ca3b02fce9223d3e734ab9b.
      // Libfuse3 dropped this option because it's default
      String bigWritesOptions = "big_writes";
      options.add(bigWritesOptions);
      LOG.info("Added fuse mount option {} to enlarge single write request size", bigWritesOptions);
    }
    if (!conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
      String directIOOptions = "direct_io";
      options.add(directIOOptions);
      LOG.info("Added fuse mount option {} for JNR FUSE", directIOOptions);
    }
    return Collections.unmodifiableSet(options);
  }

  /**
   * Creates a new instance of {@link FuseOptions}.
   *
   * @param fileSystemOptions the file system options
   * @param fuseMountOptions the FUSE mount options
   * @param updateCheckEnabled whether to enable update check
   */
  private FuseOptions(FileSystemOptions fileSystemOptions,
      Set<String> fuseMountOptions, boolean updateCheckEnabled) {
    mFileSystemOptions = Preconditions.checkNotNull(fileSystemOptions);
    mFuseMountOptions = Preconditions.checkNotNull(fuseMountOptions);
    mUpdateCheckEnabled = updateCheckEnabled;
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
}
