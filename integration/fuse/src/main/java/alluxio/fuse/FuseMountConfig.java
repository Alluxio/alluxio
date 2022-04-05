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
 * Convenience class to pass around Alluxio-FUSE options.
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
    return create(null, null, ImmutableList.of(), conf);
  }

  /**
   * Constructs a new Fuse mount configuration.
   *
   * @param mountPoint the path to where the FS should be mounted
   * @param alluxioPath the path within alluxio that will be used as the mounted FS root
   * @param fuseOptions the fuse mount options
   * @param conf the alluxio configuration to get default mount confi from
   * @return fuse mount configuration
   */
  public static FuseMountConfig create(@Nullable String mountPoint, @Nullable String alluxioPath,
        List<String> fuseOptions, AlluxioConfiguration conf) {
    if (mountPoint == null || mountPoint.isEmpty()) {
      mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
      Preconditions.checkArgument(mountPoint != null && !mountPoint.isEmpty(),
          "Fuse mount point should be set");
    }
    if (alluxioPath == null || alluxioPath.isEmpty()) {
      alluxioPath = conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH);
      Preconditions.checkArgument(alluxioPath != null && !alluxioPath.isEmpty(),
          "Fuse mount alluxio path should be set");
    }
    if (fuseOptions.isEmpty() && conf.isSet(PropertyKey.FUSE_MOUNT_OPTIONS)) {
      fuseOptions = conf.getList(PropertyKey.FUSE_MOUNT_OPTIONS);
    }
    fuseOptions = AlluxioFuse.parseFuseOptions(fuseOptions, conf);
    return new FuseMountConfig(mountPoint, alluxioPath, fuseOptions,
        conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED));
  }

  private FuseMountConfig(String mountPoint, String mountAlluxioPath,
      List<String> fuseOptions, boolean debug) {
    mMountPoint = mountPoint;
    mAlluxioPath = mountAlluxioPath;
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
