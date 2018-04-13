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
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The information of the Alluxio leader master.
 */
@NotThreadSafe
public final class MasterInfo implements Serializable {
  private static final long serialVersionUID = 5846173765139223974L;

  private String mMasterAddress;
  private int mRpcPort;
  private boolean mSafeMode;
  private long mStartTimeMs;
  private long mUpTimeMs;
  private String mVersion;
  private int mWebPort;
  private List<String> mZookeeperAddresses;

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
    mRpcPort = masterInfo.getRpcPort();
    mSafeMode = masterInfo.isSafeMode();
    mStartTimeMs = masterInfo.getStartTimeMs();
    mUpTimeMs = masterInfo.getUpTimeMs();
    mVersion = masterInfo.getVersion();
    mWebPort = masterInfo.getWebPort();
    mZookeeperAddresses = masterInfo.getZookeeperAddresses();
  }

  /**
   * @return the master address
   */
  public String getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * @return the rpc port
   */
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * @return if the cluster is running in safe mode
   */
  public boolean isSafeMode() {
    return mSafeMode;
  }

  /**
   * @return the cluster start time (in milliseconds)
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the cluster uptime (in milliseconds)
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
   * @return the web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @return the Zookeeper addresses
   */
  public List<String> getZookeeperAddresses() {
    return mZookeeperAddresses;
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
   * @param rpcPort the master address to use
   * @return the master information
   */
  public MasterInfo setRpcPort(int rpcPort) {
    mRpcPort = rpcPort;
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
   * @param webPort the web port to use
   * @return the master information
   */
  public MasterInfo setWebPort(int webPort) {
    mWebPort = webPort;
    return this;
  }

  /**
   * @param zookeeperAddresses the Zookeeper addresses to use
   * @return the master information
   */
  public MasterInfo setZookeeperAddresses(List<String> zookeeperAddresses) {
    mZookeeperAddresses = zookeeperAddresses;
    return this;
  }

  /**
   * @return thrift representation of the master information
   */
  protected alluxio.thrift.MasterInfo toThrift() {
    return new alluxio.thrift.MasterInfo().setMasterAddress(mMasterAddress)
        .setRpcPort(mRpcPort).setSafeMode(mSafeMode).setStartTimeMs(mStartTimeMs)
        .setUpTimeMs(mUpTimeMs).setVersion(mVersion).setWebPort(mWebPort)
        .setZookeeperAddresses(mZookeeperAddresses);
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
    return mMasterAddress.equals(that.mMasterAddress) && mRpcPort == that.mRpcPort
        && mSafeMode == that.mSafeMode && mStartTimeMs == that.mStartTimeMs
        && mUpTimeMs == that.mUpTimeMs && mVersion.equals(that.mVersion)
        && mWebPort == that.mWebPort && mZookeeperAddresses.equals(that.mZookeeperAddresses);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMasterAddress, mRpcPort, mSafeMode,
        mStartTimeMs, mUpTimeMs, mVersion, mWebPort, mZookeeperAddresses);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("masterAddress", mMasterAddress)
        .add("rpcPort", mRpcPort).add("safeMode", mSafeMode)
        .add("startTimeMs", mStartTimeMs).add("upTimeMs", mUpTimeMs)
        .add("version", mVersion).add("webPort", mWebPort)
        .add("zookeeperAddress", mZookeeperAddresses).toString();
  }

  /**
   * Enum representing the fields of the master info.
   */
  public static enum MasterInfoField {
    MASTER_ADDRESS,
    RPC_PORT,
    SAFE_MODE,
    START_TIME_MS,
    UP_TIME_MS,
    VERSION,
    WEB_PORT,
    ZOOKEEPER_ADDRESSES;

    /**
     * @return the thrift representation of this master info field
     */
    public alluxio.thrift.MasterInfoField toThrift() {
      return alluxio.thrift.MasterInfoField.valueOf(name());
    }

    /**
     * @param field the thrift representation of the master info field to create
     * @return the wire type version of the master info field
     */
    public static MasterInfoField fromThrift(alluxio.thrift.MasterInfoField field) {
      return MasterInfoField.valueOf(field.name());
    }
  }
}
