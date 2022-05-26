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

import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience class to create Fuse mount configuration.
 */
@ThreadSafe
public final class FuseMountConfig {
  private final String mMountPoint;
  private final String mAlluxioPath;
  private final boolean mDebug;
  private final List<String> mFuseOptions;

  /**
   * Constructs a new Fuse mount configuration.
   *
   * @param conf the alluxio configuration to get default mount config from
   * @return fuse mount configuration
   */
  public static FuseMountConfig create(AlluxioConfiguration conf) {
    Preconditions.checkNotNull(conf, "alluxio configuration");
    return create(null, null, null, conf);
  }

  /**
   * Constructs a new Fuse mount configuration.
   *
   * @param mountPoint the path to where the FS should be mounted,
   *                   use configuration if not provided
   * @param alluxioPath the path within alluxio that will be used as the mounted FS root,
   *                    use configuration if not provided
   * @param fuseOptions the fuse mount options, use configuration if not provided
   * @param conf the alluxio configuration to get mount config from
   * @return fuse mount configuration
   */
  public static FuseMountConfig create(@Nullable String mountPoint, @Nullable String alluxioPath,
        @Nullable List<String> fuseOptions, AlluxioConfiguration conf) {
    Preconditions.checkNotNull(conf, "alluxio configuration");
    if (mountPoint == null || mountPoint.isEmpty()) {
      mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    }
    if (alluxioPath == null || alluxioPath.isEmpty()) {
      alluxioPath = conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH);
    }
    if (fuseOptions == null) {
      if (conf.isSet(PropertyKey.FUSE_MOUNT_OPTIONS)) {
        fuseOptions = conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS);
      } else {
        fuseOptions = ImmutableList.of();
      }
    }
    fuseOptions = AlluxioFuse.parseFuseOptions(fuseOptions, conf);
    return new FuseMountConfig(mountPoint, alluxioPath, fuseOptions,
        conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED));
  }

  private FuseMountConfig(String mountPoint, String alluxioPath,
      List<String> fuseOptions, boolean debug) {
    Preconditions.checkArgument(mountPoint != null && !mountPoint.isEmpty(),
        "Fuse mount point should be set");
    Preconditions.checkArgument(alluxioPath != null && !alluxioPath.isEmpty(),
        "Fuse mount alluxio path should be set");
    Preconditions.checkNotNull(fuseOptions,
        "Fuse options cannot be null");
    mMountPoint = mountPoint;
    mAlluxioPath = alluxioPath;
    mFuseOptions = fuseOptions;
    mDebug = debug;
  }

  /**
   * @return The path to where the FS should be mounted
   */
  public String getMountPoint() {
    return mMountPoint;
  }

  /**
   * @return The path within alluxio that will be mounted to the local mount point
   */
  public String getMountAlluxioPath() {
    return mAlluxioPath;
  }

  /**
   * @return extra options to pass to the FUSE mount command
   */
  public List<String> getFuseMountOptions() {
    return mFuseOptions;
  }

  /**
   * @return whether the file system should be mounted in debug mode
   */
  public boolean isDebug() {
    return mDebug;
  }
}
