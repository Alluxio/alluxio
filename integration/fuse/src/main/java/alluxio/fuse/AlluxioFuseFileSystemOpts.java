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

package alluxio.fuse;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * This is an object containing all fuse-related options that
 * either can be configured through command line when launching Alluxio fuse,
 * or can be passed to Libfuse,
 * or both.
 */
public final class AlluxioFuseFileSystemOpts {
  private final String mAlluxioPath;
  private final List<String> mFuseOptions;
  private final boolean mIsDebug;
  private final String mMountPoint;

  private AlluxioFuseFileSystemOpts(
      String alluxioPath, String mountPoint, List<String> fuseOptions, boolean isDebug) {
    mAlluxioPath = alluxioPath;
    mMountPoint = mountPoint;
    mFuseOptions = fuseOptions;
    mIsDebug = isDebug;
  }

  /**
   * Constructs an AlluxioFuseFileSystemOpts with only Alluxio cluster configuration.
   *
   * @param conf Alluxio cluster configuration
   * @return AlluxioFuseFileSystemOpts
   */
  public static AlluxioFuseFileSystemOpts create(AlluxioConfiguration conf) {
    List<String> fuseOptions = conf.isSet(PropertyKey.FUSE_MOUNT_OPTIONS)
        ? conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS)
        : ImmutableList.of();
    return new AlluxioFuseFileSystemOpts(
        conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH),
        conf.getString(PropertyKey.FUSE_MOUNT_POINT),
        fuseOptions,
        conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED)
    );
  }

  /**
   * Constructs an AlluxioFuseFileSystemOpts with Alluxio cluster configuration and command line.
   * Command line input has higher precedence if a property is set both in config and command.
   *
   * @param conf Alluxio cluster configuration
   * @param fuseCliOpts Alluxio fuse command line input
   * @return AlluxioFuseFileSystemOpts
   */
  public static AlluxioFuseFileSystemOpts create(
      AlluxioConfiguration conf, AlluxioFuseCliOpts fuseCliOpts) {
    String alluxioPath = fuseCliOpts.getMountAlluxioPath();
    if (alluxioPath == null) {
      alluxioPath = conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH);
    }
    String mountPoint = fuseCliOpts.getMountPoint();
    if (mountPoint == null) {
      mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    }
    List<String> fuseOptions = fuseCliOpts.getFuseOptions();
    if (fuseOptions == null) {
      if (conf.isSet(PropertyKey.FUSE_MOUNT_OPTIONS)) {
        fuseOptions = conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS);
      } else {
        fuseOptions = ImmutableList.of();
      }
    }
    fuseOptions = optimizeAndFormatFuseOptions(fuseOptions);
    boolean isDebug = conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);

    return new AlluxioFuseFileSystemOpts(alluxioPath, mountPoint, fuseOptions, isDebug);
  }

  /**
   * Constructs an AlluxioFuseFileSystemOpts solely for testing purpose.
   * @param alluxioPath
   * @param mountPoint
   * @param fuseOptions
   * @param isDebug
   * @return AlluxioFuseFileSystemOpts
   */
  @VisibleForTesting
  public static AlluxioFuseFileSystemOpts create(
      String alluxioPath, String mountPoint, List<String> fuseOptions, boolean isDebug) {
    return new AlluxioFuseFileSystemOpts(alluxioPath, mountPoint, fuseOptions, isDebug);
  }

  /**
   * @return the alluxio path
   */
  public String getAlluxioPath() {
    return mAlluxioPath;
  }

  /**
   * @return the mount point
   */
  public String getMountPoint() {
    return mMountPoint;
  }

  /**
   * @return options for Libfuse
   */
  public List<String> getFuseOptions() {
    return mFuseOptions;
  }

  /**
   * @return if using debug-level logging for AlluxiJniFuseFileSystem and Libfuse
   */
  public boolean isDebug() {
    return mIsDebug;
  }

  private static List<String> optimizeAndFormatFuseOptions(List<String> fuseOpts) {
    List<String> fuseOptsResult = new ArrayList<>();

    // Without option big_write, the kernel limits a single writing request to 4k.
    // With option big_write, maximum of a single writing request is 128k.
    // See https://github.com/libfuse/libfuse/blob/fuse_2_9_3/ChangeLog#L655-L659,
    // and https://github.com/torvalds/linux/commit/78bb6cb9a890d3d50ca3b02fce9223d3e734ab9b.
    // Libfuse3 dropped this option because it's default. Include it doesn't error out.
    fuseOptsResult.add("-obig_writes");

    for (final String opt : fuseOpts) {
      if (opt.isEmpty()) {
        continue;
      }
      fuseOptsResult.add("-o" + opt);
    }

    return fuseOptsResult;
  }
}
