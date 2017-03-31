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

package alluxio.wire;

import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Objects;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The mount point information.
 */
@NotThreadSafe
public class MountPointInfo implements Serializable {
  private static final long serialVersionUID = -2912330427506888886L;

  private static final long UNKNOWN_CAPACITY_BYTES = -1;
  private static final long UNKNOWN_USED_BYTES = -1;

  private String mUfsUri = "";
  private String mUfsType = "";
  private long mUfsCapacityBytes = UNKNOWN_CAPACITY_BYTES;
  private long mUfsUsedBytes = UNKNOWN_USED_BYTES;
  private boolean mReadOnly;
  private HashMap<String, String> mProperties = new HashMap<>();
  private boolean mShared;

  /**
   * Creates a new instance of {@link MountPointInfo}.
   */
  public MountPointInfo() {}

  /**
   * @return the uri of the under filesystem
   */
  public String getUfsUri() {
    return mUfsUri;
  }

  /**
   * @return the type of the under filesystem
   */
  public String getUfsType() {
    return mUfsType;
  }

  /**
   * @return the capacity of the under filesystem in bytes
   */
  public long getUfsCapacityBytes() {
    return mUfsCapacityBytes;
  }

  /**
   * @return the usage of the under filesystem in bytes
   */
  public long getUfsUsedBytes() {
    return mUfsUsedBytes;
  }

  /**
   * @return whether the mount point is read-only
   */
  public boolean getReadOnly() {
    return mReadOnly;
  }

  /**
   * @return properties of the mount point
   */
  public Map<String, String> getProperties() {
    return mProperties;
  }

  /**
   * @return whether the mount point is shared
   */
  public boolean getShared() {
    return mShared;
  }

  /**
   * @param uri the uri of the under filesystem to use
   * @return the mount point information
   */
  public MountPointInfo setUfsUri(String uri) {
    mUfsUri = uri;
    return this;
  }

  /**
   * @param type the type of the under filesystem to use
   * @return the mount point information
   */
  public MountPointInfo setUfsType(String type) {
    mUfsType = type;
    return this;
  }

  /**
   * @param capacity the capacity of the under filesystem to use
   * @return the mount point information
   */
  public MountPointInfo setUfsCapacityBytes(long capacity) {
    mUfsCapacityBytes = capacity;
    return this;
  }

  /**
   * @param usedBytes the used bytes of the under filesystem to use
   * @return the mount point information
   */
  public MountPointInfo setUfsUsedBytes(long usedBytes) {
    mUfsUsedBytes = usedBytes;
    return this;
  }

  /**
   * @param readOnly the indicator of whether the mount point is read-only to use
   * @return the mount point information
   */
  public MountPointInfo setReadOnly(boolean readOnly) {
    mReadOnly = readOnly;
    return this;
  }

  /**
   * @param properties the mount point properties to use
   * @return the mount point information
   */
  public MountPointInfo setProperties(Map<String, String> properties) {
    mProperties = new HashMap<>(properties);
    return this;
  }

  /**
   * @param shared the indicator of whether the mount point is shared with all Alluxio users
   * @return the mount point information
   */
  public MountPointInfo setShared(boolean shared) {
    mShared = shared;
    return this;
  }

  /**
   * Sets information related to under filesystem, including its uri, type, storage usage.
   *
   * @param ufsUri the under filesystem uri
   */
  public void setUfsInfo(String ufsUri) {
    mUfsUri = ufsUri;
    UnderFileSystem ufs = UnderFileSystem.Factory.get(mUfsUri);
    mUfsType = ufs.getUnderFSType();
    try {
      mUfsCapacityBytes = ufs.getSpace(mUfsUri, UnderFileSystem.SpaceType.SPACE_TOTAL);
    } catch (IOException e) {
      mUfsCapacityBytes = UNKNOWN_CAPACITY_BYTES;
    }
    try {
      mUfsUsedBytes = ufs.getSpace(mUfsUri, UnderFileSystem.SpaceType.SPACE_USED);
    } catch (IOException e) {
      mUfsUsedBytes = UNKNOWN_USED_BYTES;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MountPointInfo)) {
      return false;
    }
    MountPointInfo that = (MountPointInfo) o;
    return mUfsUri.equals(that.mUfsUri) && mUfsType.equals(that.mUfsType)
        && mUfsCapacityBytes == that.mUfsCapacityBytes && mUfsUsedBytes == that.mUfsUsedBytes
        && mReadOnly == that.mReadOnly && mProperties.equals(that.mProperties)
        && mShared == that.mShared;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUfsUri, mUfsType, mUfsCapacityBytes, mUfsUsedBytes, mReadOnly,
        mProperties, mShared);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("ufsUrl", mUfsUri).add("ufsType", mUfsType)
        .add("ufsCapacityBytes", mUfsCapacityBytes).add("ufsUsedBytes", mUfsUsedBytes)
        .add("readOnly", mReadOnly).add("properties", mProperties)
        .add("shared", mShared).toString();
  }
}
