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

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience class to pass around Alluxio-FUSE options.
 */
@ThreadSafe
public final class FuseMountOptions {
  private final String mMountPoint;
  private final String mAlluxioRoot;
  private final boolean mDebug;
  private final List<String> mFuseOpts;

  /**
   * @param mountPoint the path to where the FS should be mounted
   * @param alluxioRoot the path within alluxio that will be used as the mounted FS root
   * @param debug whether the file system should be mounted in debug mode
   * @param fuseOpts extra options to pass to the FUSE mount command
   */
  public FuseMountOptions(String mountPoint, String alluxioRoot,
      boolean debug, List<String> fuseOpts) {
    mMountPoint = mountPoint;
    mAlluxioRoot = alluxioRoot;
    mDebug = debug;
    mFuseOpts = fuseOpts;
  }

  /**
   * @return The path to where the FS should be mounted
   */
  public String getMountPoint() {
    return mMountPoint;
  }

  /**
   * @return The path within alluxio that will be used as the mounted FS root
   * (e.g. /users/andrea)
   */
  public String getAlluxioRoot() {
    return mAlluxioRoot;
  }

  /**
   * @return extra options to pass to the FUSE mount command
   */
  public List<String> getFuseOpts() {
    return mFuseOpts;
  }

  /**
   * @return whether the file system should be mounted in debug mode
   */
  public boolean isDebug() {
    return mDebug;
  }
}
