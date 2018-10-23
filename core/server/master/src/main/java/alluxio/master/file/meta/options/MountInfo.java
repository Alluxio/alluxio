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

package alluxio.master.file.meta.options;

import alluxio.AlluxioURI;
import alluxio.master.file.options.MountOptions;
import alluxio.wire.MountPointInfo;

import com.google.common.base.Preconditions;

import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class holds information about a mount point.
 */
@ThreadSafe
public class MountInfo {
  private final AlluxioURI mAlluxioUri;
  private final AlluxioURI mUfsUri;
  private final MountOptions mOptions;
  private final long mMountId;

  /**
   * Creates a new instance of {@code MountInfo}.
   *
   * @param alluxioUri an Alluxio path URI
   * @param ufsUri a UFS path URI
   * @param mountId the id of the mount
   * @param options the mount options
   */
  public MountInfo(AlluxioURI alluxioUri, AlluxioURI ufsUri, long mountId, MountOptions options) {
    mAlluxioUri = Preconditions.checkNotNull(alluxioUri, "alluxioUri");
    mUfsUri = Preconditions.checkNotNull(ufsUri, "ufsUri");
    mMountId = mountId;
    mOptions = options;
  }

  /**
   * @return the {@link AlluxioURI} of the Alluxio path
   */
  public AlluxioURI getAlluxioUri() {
    return mAlluxioUri;
  }

  /**
   * @return the {@link AlluxioURI} of the ufs path
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

  /**
   * @return the id of the mount
   */
  public long getMountId() {
    return mMountId;
  }

  /**
   * @return the {@link MountPointInfo} for the mount point
   */
  public MountPointInfo toMountPointInfo() {
    MountPointInfo info = new MountPointInfo();
    info.setUfsUri(mUfsUri.toString());
    info.setReadOnly(mOptions.isReadOnly());
    info.setProperties(mOptions.getProperties());
    info.setShared(mOptions.isShared());
    return info;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MountInfo)) {
      return false;
    }
    MountInfo that = (MountInfo) o;
    return mMountId == that.getMountId()
        && mAlluxioUri.equals(that.getAlluxioUri())
        && mUfsUri.equals(that.getUfsUri())
        && mOptions.equals(that.getOptions());
  }

  @Override
  public int hashCode() {
    return Objects.hash(mMountId, mAlluxioUri, mUfsUri, mOptions);
  }
}
