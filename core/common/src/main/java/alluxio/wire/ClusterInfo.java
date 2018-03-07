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

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The Alluxio cluster information.
 */
@NotThreadSafe
public final class ClusterInfo implements Serializable {

  private String mMasterAddress;
  private String mStartTime;
  private String mUpTime;
  private String mVersion;
  private boolean mSafeMode;

  /**
   * Creates a new instance of {@link ClusterInfo}.
   */
  public ClusterInfo() {}

  /**
   * Creates a new instance of {@link ClusterInfo} from a thrift representation.
   *
   * @param clusterInfo the thrift representation of a worker information
   */
  protected ClusterInfo(alluxio.thrift.ClusterInfo clusterInfo) {
    mMasterAddress = clusterInfo.getMasterAddress();
    mStartTime = clusterInfo.getStartTime();
    mUpTime = clusterInfo.getUpTime();
    mVersion = clusterInfo.getVersion();
    mSafeMode = clusterInfo.isSafeMode();
  }

  /**
   * @return the master address
   */
  public String getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * @return the cluster last time (in milliseconds)
   */
  public String getUpTime() {
    return mUpTime;
  }

  /**
   * @return the runtime version
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * @return if the cluster is running in safe mode
   */
  public boolean isSafeMode() {
    return mSafeMode;
  }

  /**
   * @return the cluster start time
   */
  public String getStartTime() {
    return mStartTime;
  }

  /**
   * @param version the version to use
   * @return the cluster information
   */
  public ClusterInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * @param masterAddress the master address to use
   * @return the cluster information
   */
  public ClusterInfo setMasterAddress(String masterAddress) {
    mMasterAddress = masterAddress;
    return this;
  }

  /**
   * @param upTime the upTime to use
   * @return the cluster information
   */
  public ClusterInfo setUpTime(String upTime) {
    mUpTime = upTime;
    return this;
  }

  /**
   * @param startTime the startTime to use
   * @return the cluster information
   */
  public ClusterInfo setStartTime(String startTime) {
    mStartTime = startTime;
    return this;
  }

  /**
   * @param safeMode the safeMode to use
   * @return the cluster information
   */
  public ClusterInfo setSafeMode(boolean safeMode) {
    mSafeMode = safeMode;
    return this;
  }

  /**
   * @return thrift representation of the cluster information
   */
  protected alluxio.thrift.ClusterInfo toThrift() {
    return new alluxio.thrift.ClusterInfo(mMasterAddress, mStartTime, mUpTime, mVersion, mSafeMode);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusterInfo)) {
      return false;
    }
    ClusterInfo that = (ClusterInfo) o;
    return mMasterAddress.equals(that.mMasterAddress) && mStartTime.equals(that.mStartTime)
        && mUpTime.equals(that.mUpTime) && mVersion.equals(that.mVersion)
        && mSafeMode == that.mSafeMode;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMasterAddress, mStartTime, mUpTime, mVersion, mSafeMode);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("masterAddress", mMasterAddress)
        .add("startTime", mStartTime).add("upTimeMs", mUpTime)
        .add("version", mVersion)
        .add("safeMode", mSafeMode).toString();
  }
}
