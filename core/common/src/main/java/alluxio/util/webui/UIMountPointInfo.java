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

package alluxio.util.webui;

import alluxio.util.FormatUtils;
import alluxio.wire.MountPointInfo;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Contains information about a mount point to be displayed in the UI.
 */
@ThreadSafe
public class UIMountPointInfo {
  private String mMountPoint = "";
  private String mUfsUri = "";
  private String mUfsType = "";
  private long mUfsCapacityBytes;
  private long mUfsUsedBytes;
  private boolean mReadOnly;
  private boolean mShared;
  private String mProperties = "";

  /**
   * Instantiates a new instance of {@link UIMountPointInfo}.
   *
   * @param mountPoint the mount point
   * @param mountPointInfo the {@link MountPointInfo} for the mount point
   */
  public UIMountPointInfo(String mountPoint, MountPointInfo mountPointInfo) {
    mMountPoint = mountPoint;
    mUfsUri = mountPointInfo.getUfsUri();
    mUfsType = mountPointInfo.getUfsType();
    mUfsCapacityBytes = mountPointInfo.getUfsCapacityBytes();
    mUfsUsedBytes = mountPointInfo.getUfsUsedBytes();
    mReadOnly = mountPointInfo.getReadOnly();
    mShared = mountPointInfo.getShared();
    if (mountPointInfo.getProperties() != null) {
      mProperties = mountPointInfo.getProperties().toString();
    }
  }

  /**
   * Gets ufs capacity bytes.
   *
   * @return the ufs capacity bytes
   */
  public String getUfsCapacityBytes() {
    return FormatUtils.getSizeFromBytes(mUfsCapacityBytes);
  }

  /**
   * Gets ufs used bytes.
   *
   * @return the ufs used bytes
   */
  public String getUfsUsedBytes() {
    return FormatUtils.getSizeFromBytes(mUfsUsedBytes);
  }

  /**
   * Gets mount point.
   *
   * @return the mount point
   */
  public String getMountPoint() {
    return mMountPoint;
  }

  /**
   * Gets ufs type.
   *
   * @return the ufs type
   */
  public String getUfsType() {
    return mUfsType;
  }

  /**
   * Gets ufs uri.
   *
   * @return the ufs uri
   */
  public String getUfsUri() {
    return mUfsUri;
  }

  /**
   * Is readonly.
   *
   * @return whether the mount point is readonly
   */
  public boolean isReadOnly() {
    return mReadOnly;
  }

  /**
   * Is shared.
   *
   * @return whether the mount point is shared
   */
  public boolean isShared() {
    return mShared;
  }

  /**
   * @return the properties
   */
  public String getProperties() {
    return mProperties;
  }
}
