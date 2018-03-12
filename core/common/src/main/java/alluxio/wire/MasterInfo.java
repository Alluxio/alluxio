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
 * The information of the Alluxio leader master.
 */
@NotThreadSafe
public final class MasterInfo implements Serializable {
  private static final long serialVersionUID = -4151145809689718450L;

  private String mMasterAddress;
  private int mWebPort;
  private int mRpcPort;
  private long mStartTimeMs;
  private long mUpTimeMs;
  private String mVersion;
  private boolean mSafeMode;

  /**
   * Creates a new instance of {@link MasterInfo}.
   */
  public MasterInfo() {}

  /**
   * Creates a new instance of {@link MasterInfo} from a thrift representation.
   *
   * @param masterInfo the thrift representation of alluxio master information
   */
  protected MasterInfo(alluxio.thrift.MasterInfo masterInfo) {
    mMasterAddress = masterInfo.getMasterAddress();
    mWebPort = masterInfo.getWebPort();
    mRpcPort = masterInfo.getRpcPort();
    mStartTimeMs = masterInfo.getStartTimeMs();
    mUpTimeMs = masterInfo.getUpTimeMs();
    mVersion = masterInfo.getVersion();
    mSafeMode = masterInfo.isSafeMode();
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
   * @return the cluster start time (in milliseconds)
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the cluster last time (in milliseconds)
   */
  public long getUpTimeMs() {
    return mUpTimeMs;
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
   * @param masterAddress the master address to use
   * @return the master information
   */
  public MasterInfo setMasterAddress(String masterAddress) {
    mMasterAddress = masterAddress;
    return this;
  }

  /**
   * @param webPort the web port to use
   * @return the master information
   */
  public MasterInfo setWebPort(int webPort) {
    mWebPort = webPort;
    return this;
  }

  /**
   * @param rpcPort the master address to use
   * @return the master information
   */
  public MasterInfo setRpcPort(int rpcPort) {
    mRpcPort = rpcPort;
    return this;
  }

  /**
   * @param startTimeMs the start time in milliseconds to use
   * @return the master information
   */
  public MasterInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param upTimeMs the up time in milliseconds to use
   * @return the master information
   */
  public MasterInfo setUpTimeMs(long upTimeMs) {
    mUpTimeMs = upTimeMs;
    return this;
  }

  /**
   * @param version the version to use
   * @return the master information
   */
  public MasterInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * @param safeMode whether Alluxio is in safe mode or not
   * @return the master information
   */
  public MasterInfo setSafeMode(boolean safeMode) {
    mSafeMode = safeMode;
    return this;
  }

  /**
   * @return thrift representation of the master information
   */
  protected alluxio.thrift.MasterInfo toThrift() {
    return new alluxio.thrift.MasterInfo(mMasterAddress, mWebPort, mRpcPort,
        mStartTimeMs, mUpTimeMs, mVersion, mSafeMode);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MasterInfo)) {
      return false;
    }
    MasterInfo that = (MasterInfo) o;
    return mMasterAddress.equals(that.mMasterAddress) && mWebPort == that.mWebPort
        && mRpcPort == that.mRpcPort && mStartTimeMs == that.mStartTimeMs
        && mUpTimeMs == that.mUpTimeMs && mVersion.equals(that.mVersion)
        && mSafeMode == that.mSafeMode;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMasterAddress, mWebPort,
        mRpcPort, mStartTimeMs, mUpTimeMs, mVersion, mSafeMode);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("masterAddress", mMasterAddress)
        .add("webPort", mWebPort).add("rpcPort", mRpcPort)
        .add("startTimeMs", mStartTimeMs).add("upTimeMs", mUpTimeMs)
        .add("version", mVersion).add("safeMode", mSafeMode).toString();
  }
}
