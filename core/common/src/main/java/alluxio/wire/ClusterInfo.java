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
  private static final long serialVersionUID = -4151145809689718450L;

  private String mMasterAddress;
  private int mWebPort;
  private int mRpcPort;
  private String mStartTime;
  private String mUpTime;
  private String mVersion;
  private boolean mHAMode;
  private boolean mSafeMode;

  /**
   * Creates a new instance of {@link ClusterInfo}.
   */
  public ClusterInfo() {}

  /**
   * Creates a new instance of {@link ClusterInfo} from a thrift representation.
   *
   * @param clusterInfo the thrift representation of a alluxio cluster information
   */
  protected ClusterInfo(alluxio.thrift.ClusterInfo clusterInfo) {
    mMasterAddress = clusterInfo.getMasterAddress();
    mWebPort = clusterInfo.getWebPort();
    mRpcPort = clusterInfo.getRpcPort();
    mStartTime = clusterInfo.getStartTime();
    mUpTime = clusterInfo.getUpTime();
    mVersion = clusterInfo.getVersion();
    mHAMode = clusterInfo.isHAMode();
    mSafeMode = clusterInfo.isSafeMode();
  }

  /**
   * @return the master address
   */
  public String getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * @return the web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @return the Rpc port
   */
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * @return the cluster start time
   */
  public String getStartTime() {
    return mStartTime;
  }

  /**
   * @return the cluster last time
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
   * @return if the cluster is running in high availability mode
   */
  public boolean isHAMode() {
    return mHAMode;
  }

  /**
   * @return if the cluster is running in safe mode
   */
  public boolean isSafeMode() {
    return mSafeMode;
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
   * @param webPort the web port to use
   * @return the cluster information
   */
  public ClusterInfo setWebPort(int webPort) {
    mWebPort = webPort;
    return this;
  }

  /**
   * @param rpcPort the master address to use
   * @return the cluster information
   */
  public ClusterInfo setRpcPort(int rpcPort) {
    mRpcPort = rpcPort;
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
   * @param upTime the upTime to use
   * @return the cluster information
   */
  public ClusterInfo setUpTime(String upTime) {
    mUpTime = upTime;
    return this;
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
   * @param HAMode whether Alluxio is in high availability mode or not
   * @return the cluster information
   */
  public ClusterInfo setHAMode(boolean HAMode) {
    mHAMode = HAMode;
    return this;
  }

  /**
   * @param safeMode whether Alluxio is in safe mode or not
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
    return new alluxio.thrift.ClusterInfo(mMasterAddress, mWebPort, mRpcPort,
        mStartTime, mUpTime, mVersion, mHAMode, mSafeMode);
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
    return mMasterAddress.equals(that.mMasterAddress) && mWebPort == that.mWebPort
        && mRpcPort == that.mRpcPort && mStartTime.equals(that.mStartTime)
        && mUpTime.equals(that.mUpTime) && mVersion.equals(that.mVersion)
        && mHAMode == that.mHAMode && mSafeMode == that.mSafeMode;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMasterAddress, mWebPort,
        mRpcPort, mStartTime, mUpTime, mVersion, mHAMode, mSafeMode);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("masterAddress", mMasterAddress)
        .add("webPort", mWebPort).add("rpcPort", mRpcPort)
        .add("startTime", mStartTime).add("upTimeMs", mUpTime)
        .add("version", mVersion).add("HAMode", mHAMode)
        .add("safeMode", mSafeMode).toString();
  }
}
