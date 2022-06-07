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
  private final Class mFuseAuthPolicyClass;
  private final Optional<String> mFuseAuthPolicyCustomGroup;
  private final Optional<String> mFuseAuthPolicyCustomUser;
  private final int mFuseMaxPathCached;
  private final int mFuseUmountTimeout;
  private final boolean mIsDebug;
  private final List<String> mLibfuseOptions;
  private final boolean mMetadataCacheEnabled;
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
    Preconditions.checkNotNull(conf,
        "AlluxioConfiguration should not be null for creating AlluxioFuseFileSystemOpts.");
    List<String> libfuseOptions = conf.isSet(PropertyKey.FUSE_MOUNT_OPTIONS)
        ? conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS)
        : ImmutableList.of();
    Optional<String> authPolicyCustomGroup = conf.isSet(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP)
        ? Optional.of(conf.getString(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP))
        : Optional.empty();
    Optional<String> authPolicyCustomUser = conf.isSet(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER)
        ? Optional.of(conf.getString(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER))
        : Optional.empty();
    return new AlluxioFuseFileSystemOpts(
        conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH),
        conf.getString(PropertyKey.FUSE_FS_NAME),
        conf.getClass(PropertyKey.FUSE_AUTH_POLICY_CLASS),
        authPolicyCustomGroup,
        authPolicyCustomUser,
        conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX),
        (int) conf.getMs(PropertyKey.FUSE_UMOUNT_TIMEOUT),
        conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED),
        libfuseOptions,
        conf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED),
        conf.getString(PropertyKey.FUSE_MOUNT_POINT),
        conf.getBoolean(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED),
        conf.getMs(PropertyKey.FUSE_STAT_CACHE_REFRESH_INTERVAL),
        conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED)
    );
  }

  /**SystemUserGroupAuthPolicy.java
   * Constructs an {@link AlluxioFuseFileSystemOpts} with
   * Alluxio cluster configuration and command line input.
   * Command line input has higher precedence if a property is set both in config and command.
   *
   * @param conf Alluxio cluster configuration
   * @param fuseCliOpts Alluxio fuse command line input
   * @return AlluxioFuseFileSystemOpts
   */
  public static AlluxioFuseFileSystemOpts create(
      AlluxioConfiguration conf, AlluxioFuseCliOpts fuseCliOpts) {
    Preconditions.checkNotNull(conf,
        "AlluxioConfiguration should not be null for creating AlluxioFuseFileSystemOpts.");
    Preconditions.checkNotNull(fuseCliOpts,
        "AlluxioFuseCliOpts should not be null for creating AlluxioFuseFileSystemOpts.");
    String alluxioPath = fuseCliOpts.getMountAlluxioPath().orElseGet(
        () -> conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH));
    Optional<String> authPolicyCustomGroup = conf.isSet(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP)
        ? Optional.of(conf.getString(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP))
        : Optional.empty();
    Optional<String> authPolicyCustomUser = conf.isSet(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER)
        ? Optional.of(conf.getString(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER))
        : Optional.empty();
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
    return new AlluxioFuseFileSystemOpts(alluxioPath,
        conf.getString(PropertyKey.FUSE_FS_NAME),
        conf.getClass(PropertyKey.FUSE_AUTH_POLICY_CLASS),
        authPolicyCustomGroup,
        authPolicyCustomUser,
        conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX),
        (int) conf.getMs(PropertyKey.FUSE_UMOUNT_TIMEOUT),
        conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED),
        libfuseOptions,
        conf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED),
        mountPoint,
        conf.getBoolean(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED),
        conf.getMs(PropertyKey.FUSE_STAT_CACHE_REFRESH_INTERVAL),
        conf.getBoolean(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED));
  }

  private AlluxioFuseFileSystemOpts(String alluxioPath, String fsName, Class fuseAuthPolicyClass,
        Optional<String> fuseAuthPolicyCustomGroup, Optional<String> fuseAuthPolicyCustomUser,
        int fuseMaxPathCached, int fuseUmountTimeout, boolean isDebug, List<String> libfuseOptions,
        boolean metaDataCacheEnabled, String mountPoint, boolean specialCommandEnabled,
        long statCacheTimeout, boolean userGroupTranslationEnabled) {
    mAlluxioPath = Preconditions.checkNotNull(alluxioPath,
        "Option alluxioPath for an AlluxioFuse filesystem should not be null.");
    mFsName = Preconditions.checkNotNull(fsName,
        "Option fsName for an AlluxioFuse filesystem should not be null.");
    mFuseAuthPolicyClass = Preconditions.checkNotNull(fuseAuthPolicyClass,
        "Option fuseAuthPolicyClass for an AlluxioFuse filesystem should not be null.");
    mFuseAuthPolicyCustomGroup = Preconditions.checkNotNull(fuseAuthPolicyCustomGroup,
        "Option fuseAuthPolicyCustomGroup for an AlluxioFuse filesystem should not be null.");
    mFuseAuthPolicyCustomUser = Preconditions.checkNotNull(fuseAuthPolicyCustomUser,
        "Option fuseAuthPolicyCustomUser for an AlluxioFuse filesystem should not be null.");
    mFuseMaxPathCached = fuseMaxPathCached;
    mFuseUmountTimeout = fuseUmountTimeout;
    mIsDebug = isDebug;
    mLibfuseOptions = Preconditions.checkNotNull(libfuseOptions,
        "Option libfuseOptions for an AlluxioFuse filesystem should not be null.");
    mMetadataCacheEnabled = metaDataCacheEnabled;
    mMountPoint = Preconditions.checkNotNull(mountPoint,
        "Option mountPoint for an AlluxioFuse filesystem should not be null.");
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
   * @return the authorization policy class for Fuse
   */
  public Class getFuseAuthPolicyClass() {
    return mFuseAuthPolicyClass;
  }

  /**
   * @return the fuse group for the authorization policy
   */
  public Optional<String> getFuseAuthPolicyCustomGroup() {
    return mFuseAuthPolicyCustomGroup;
  }

  /**
   * @return the fuse user for the authorization policy
   */
  public Optional<String> getFuseAuthPolicyCustomUser() {
    return mFuseAuthPolicyCustomUser;
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
   * @return if caching metadata on client side
   */
  public boolean isMetadataCacheEnabled() {
    return mMetadataCacheEnabled;
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
