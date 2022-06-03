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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class holds all fuse-related options used by libfuse and AlluxioFuse.
 */
public final class AlluxioFuseFileSystemOpts {
  private final String mAlluxioPath;
  private final String mFsName;
  private final int mFuseMaxPathCached;
  private final int mFuseUmountTimeout;
  private final boolean mIsDebug;
  private final List<String> mLibfuseOptions;
  private final String mMountPoint;
  private final boolean mSpecialCommandEnabled;
  private final long mStatCacheTimeout;
  private final boolean mUserGroupTranslationEnabled;

  /**
   * Constructs an {@link AlluxioFuseFileSystemOpts} with only Alluxio cluster configuration.
   *
   * @param conf     Alluxio cluster configuration
   * @return AlluxioFuseFileSystemOpts
   */
  public static AlluxioFuseFileSystemOpts create(AlluxioConfiguration conf) {
    List<String> libfuseOptions = conf.isSet(PropertyKey.FUSE_MOUNT_OPTIONS)
        ? conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS)
        : ImmutableList.of();
    return new AlluxioFuseFileSystemOpts(
        conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH),
        conf.getString(PropertyKey.FUSE_FS_NAME),
        conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX),
        (int) conf.getMs(PropertyKey.FUSE_UMOUNT_TIMEOUT),
        conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED),
        libfuseOptions,
        conf.getString(PropertyKey.FUSE_MOUNT_POINT),
        conf.getBoolean(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED),
        conf.getMs(PropertyKey.FUSE_STAT_CACHE_REFRESH_INTERVAL),
        conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED)
    );
  }

  /**
   * Constructs an {@link AlluxioFuseFileSystemOpts} with
   * Alluxio cluster configuration and command line.
   * Command line input has higher precedence if a property is set both in config and command.
   *
   * @param conf     Alluxio cluster configuration
   * @param fuseCliOpts     Alluxio fuse command line input
   * @return AlluxioFuseFileSystemOpts
   */
  public static AlluxioFuseFileSystemOpts create(
      AlluxioConfiguration conf, AlluxioFuseCliOpts fuseCliOpts) {
    Optional<String> alluxioPathFromCli = fuseCliOpts.getMountAlluxioPath();
    String alluxioPath = alluxioPathFromCli.orElseGet(
        () -> conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH));
    String fsName = conf.getString(PropertyKey.FUSE_FS_NAME);
    int fuseMaxPathCached = conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
    int fuseUmountTimeout = (int) conf.getMs(PropertyKey.FUSE_UMOUNT_TIMEOUT);
    boolean isDebug = conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);
    Optional<List<String>> libfuseOptionsFromCli = fuseCliOpts.getFuseOptions();
    List<String> libfuseOptions;
    if (libfuseOptionsFromCli.isPresent()) {
      libfuseOptions = libfuseOptionsFromCli.get();
    } else {
      if (conf.isSet(PropertyKey.FUSE_MOUNT_OPTIONS)) {
        libfuseOptions = conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS);
      } else {
        libfuseOptions = ImmutableList.of();
      }
    }
    libfuseOptions = optimizeAndFormatFuseOptions(libfuseOptions);
    Optional<String> mountPointFromCli = fuseCliOpts.getMountPoint();
    String mountPoint = mountPointFromCli.orElseGet(
        () -> conf.getString(PropertyKey.FUSE_MOUNT_POINT));
    boolean specialCommandEnabled = conf.getBoolean(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED);
    long statCacheTimeout = conf.getMs(PropertyKey.FUSE_STAT_CACHE_REFRESH_INTERVAL);
    boolean userGroupTranslationEnabled = conf.getBoolean(
        PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED);
    return new AlluxioFuseFileSystemOpts(alluxioPath, fsName, fuseMaxPathCached, fuseUmountTimeout,
        isDebug, libfuseOptions, mountPoint, specialCommandEnabled, statCacheTimeout,
        userGroupTranslationEnabled);
  }

  private AlluxioFuseFileSystemOpts(String alluxioPath, String fsName, int fuseMaxPathCached,
        int fuseUmountTimeout, boolean isDebug, List<String> libfuseOptions, String mountPoint,
        boolean specialCommandEnabled, long statCacheTimeout,
        boolean userGroupTranslationEnabled) {
    Preconditions.checkNotNull(alluxioPath,
        "Option alluxioPath for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(fsName,
        "Option fsName for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(fuseMaxPathCached,
        "Option fuseMaxPathCached for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(fuseUmountTimeout,
        "Option fuseUmountTimeout for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(isDebug,
        "Option isDebug for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(libfuseOptions,
        "Option libfuseOptions for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(mountPoint,
        "Option mountPoint for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(specialCommandEnabled,
        "Option specialCommandEnabled for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(statCacheTimeout,
        "Option statCacheTimeout for an AlluxioFuse filesystem should not be null.");
    Preconditions.checkNotNull(userGroupTranslationEnabled,
        "Option userGroupTranslationEnabled for an AlluxioFuse filesystem should not be null.");
    mAlluxioPath = alluxioPath;
    mFsName = fsName;
    mFuseMaxPathCached = fuseMaxPathCached;
    mFuseUmountTimeout = fuseUmountTimeout;
    mIsDebug = isDebug;
    mLibfuseOptions = libfuseOptions;
    mMountPoint = mountPoint;
    mSpecialCommandEnabled = specialCommandEnabled;
    mStatCacheTimeout = statCacheTimeout;
    mUserGroupTranslationEnabled = userGroupTranslationEnabled;
  }

  /**
   * @return the alluxio path
   */
  public String getAlluxioPath() {
    return mAlluxioPath;
  }

  /**
   * @return the Filesystem name
   */
  public String getFsName() {
    return mFsName;
  }

  /**
   * @return the max number of FUSE-to-Alluxio path mappings to cache
   */
  public int getFuseMaxPathCached() {
    return mFuseMaxPathCached;
  }

  /**
   * @return the wait timeout for fuse to umount
   */
  public int getFuseUmountTimeout() {
    return mFuseUmountTimeout;
  }

  /**
   * @return if using debug-level logging for AlluxiJniFuseFileSystem and Libfuse
   */
  public boolean isDebug() {
    return mIsDebug;
  }

  /**
   * @return options for Libfuse
   */
  public List<String> getFuseOptions() {
    return mLibfuseOptions;
  }

  /**
   * @return the mount point
   */
  public String getMountPoint() {
    return mMountPoint;
  }

  /**
   * @return whether AlluxioFuse special command is enabled
   */
  public boolean isSpecialCommandEnabled() {
    return mSpecialCommandEnabled;
  }

  /**
   * @return the time the result of statfs is cached
   */
  public long getStatCacheTimeout() {
    return mStatCacheTimeout;
  }

  /**
   * @return whether user and group are translated from Alluxio to Unix
   */
  public boolean isUserGroupTranslationEnabled() {
    return mUserGroupTranslationEnabled;
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
