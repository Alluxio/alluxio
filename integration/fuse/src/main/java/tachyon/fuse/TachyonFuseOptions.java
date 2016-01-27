/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.fuse;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience class to pass around Tachyon-FUSE options.
 */
@ThreadSafe
final class TachyonFuseOptions {
  private final String mMountPoint;
  private final String mTachyonRoot;
  private final boolean mDebug;
  private final List<String> mFuseOpts;

  TachyonFuseOptions(String mountPoint, String tachyonRoot,
      boolean debug, List<String> fuseOpts) {
    mMountPoint = mountPoint;
    mTachyonRoot = tachyonRoot;
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
   * @return The path within tachyon that will be used as the mounted FS root
   * (e.g. /users/andrea)
   */
  public String getTachyonRoot() {
    return mTachyonRoot;
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
