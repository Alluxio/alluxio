/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta.options;

import alluxio.AlluxioURI;
import alluxio.master.file.options.MountOptions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class holds information about a mount point.
 */
@ThreadSafe
public final class MountInfo {
  private final AlluxioURI mUfsUri;
  private final MountOptions mOptions;

  /**
   * Creates a new instance of {@code MountInfo}.
   *
   * @param ufsUri a UFS path URI
   * @param options the mount options
   */
  public MountInfo(AlluxioURI ufsUri, MountOptions options) {
    mUfsUri = ufsUri;
    mOptions = options;
  }

  /**
   * @return the {@link AlluxioURI} of ufs path
   */
  public AlluxioURI getUfsUri() {
    return mUfsUri;
  }

  /**
   * @return the {@link MountOptions} for the mount point
   */
  public MountOptions getOptions() {
    return mOptions;
  }
}
