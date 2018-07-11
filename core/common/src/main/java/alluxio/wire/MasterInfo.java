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
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The information of the Alluxio leader master.
 */
@NotThreadSafe
public final class MasterInfo implements Serializable {
  private static final long serialVersionUID = 5846173765139223974L;

  private String mLeaderMasterAddress;
  private List<Address> mMasterAddresses;
  private int mRpcPort;
  private boolean mSafeMode;
  private long mStartTimeMs;
  private long mUpTimeMs;
  private String mVersion;
  private int mWebPort;
  private List<Address> mWorkerAddresses;
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
  private MasterInfo(alluxio.thrift.MasterInfo masterInfo) {
    mLeaderMasterAddress = masterInfo.getLeaderMasterAddress();
    if (masterInfo.isSetMasterAddresses()) {
      mMasterAddresses = masterInfo.getMasterAddresses().stream()
          .map(Address::fromThrift).collect(Collectors.toList());
    }
    mRpcPort = masterInfo.getRpcPort();
    mSafeMode = masterInfo.isSafeMode();
    mStartTimeMs = masterInfo.getStartTimeMs();
    mUpTimeMs = masterInfo.getUpTimeMs();
    mVersion = masterInfo.getVersion();
    mWebPort = masterInfo.getWebPort();
    if (masterInfo.isSetWorkerAddresses()) {
      mWorkerAddresses = masterInfo.getWorkerAddresses().stream()
          .map(Address::fromThrift).collect(Collectors.toList());
    }
    mZookeeperAddresses = masterInfo.getZookeeperAddresses();
  }

  /**
   * @return the leader master address
   */
  public String getLeaderMasterAddress() {
    return mLeaderMasterAddress;
  }

  /**
   * @return the master addresses
   */
  public List<Address> getMasterAddresses() {
    return mMasterAddresses;
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
   * @return the worker addresses
   */
  public List<Address> getWorkerAddresses() {
    return mWorkerAddresses;
  }

  /**
   * @return the Zookeeper addresses
   */
  public List<String> getZookeeperAddresses() {
    return mZookeeperAddresses;
  }

  /**
   * @param leaderMasterAddress the leader master address to use
   * @return the master information
   */
  public MasterInfo setLeaderMasterAddress(String leaderMasterAddress) {
    mLeaderMasterAddress = leaderMasterAddress;
    return this;
  }

  /**
   * @param masterAddresses the master addresses to use
   * @return the master information
   */
  public MasterInfo setMasterAddresses(List<Address> masterAddresses) {
    mMasterAddresses = masterAddresses;
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
   * @param workerAddresses the worker addresses to use
   * @return the master information
   */
  public MasterInfo setWorkerAddresses(List<Address> workerAddresses) {
    mWorkerAddresses = workerAddresses;
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
    alluxio.thrift.MasterInfo masterInfo = new alluxio.thrift.MasterInfo()
        .setLeaderMasterAddress(mLeaderMasterAddress)
        .setRpcPort(mRpcPort).setSafeMode(mSafeMode).setStartTimeMs(mStartTimeMs)
        .setUpTimeMs(mUpTimeMs).setVersion(mVersion).setWebPort(mWebPort)
        .setZookeeperAddresses(mZookeeperAddresses);
    if (mMasterAddresses != null) {
      masterInfo.setMasterAddresses(mMasterAddresses.stream()
          .map(Address::toThrift).collect(Collectors.toList()));
    }
    if (mWorkerAddresses != null) {
      masterInfo.setWorkerAddresses(mWorkerAddresses.stream()
          .map(Address::toThrift).collect(Collectors.toList()));
    }
    return masterInfo;
  }

  /**
   * Creates a new instance of {@link MasterInfo} from a thrift representation.
   *
   * @param masterInfo the thrift representation of alluxio master information
   * @return the instance
   */
  public static MasterInfo fromThrift(alluxio.thrift.MasterInfo masterInfo) {
    return new MasterInfo(masterInfo);
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
    return Objects.equal(mLeaderMasterAddress, that.mLeaderMasterAddress)
        && Objects.equal(mMasterAddresses, that.mMasterAddresses) && mRpcPort == that.mRpcPort
        && mSafeMode == that.mSafeMode && mStartTimeMs == that.mStartTimeMs
        && mUpTimeMs == that.mUpTimeMs && Objects.equal(mVersion, that.mVersion)
        && mWebPort == that.mWebPort && Objects.equal(mWorkerAddresses, that.mWorkerAddresses)
        && Objects.equal(mZookeeperAddresses, that.mZookeeperAddresses);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLeaderMasterAddress, mMasterAddresses, mRpcPort,
        mSafeMode, mStartTimeMs, mUpTimeMs, mVersion, mWebPort,
        mWorkerAddresses, mZookeeperAddresses);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("leaderMasterAddress", mLeaderMasterAddress)
        .add("masterAddresses", mMasterAddresses).add("rpcPort", mRpcPort)
        .add("safeMode", mSafeMode).add("startTimeMs", mStartTimeMs)
        .add("upTimeMs", mUpTimeMs).add("version", mVersion)
        .add("webPort", mWebPort).add("workerAddresses", mWorkerAddresses)
        .add("zookeeperAddress", mZookeeperAddresses).toString();
  }

  /**
   * Enum representing the fields of the master info.
   */
  public static enum MasterInfoField {
    LEADER_MASTER_ADDRESS,
    MASTER_ADDRESSES,
    RPC_PORT,
    SAFE_MODE,
    START_TIME_MS,
    UP_TIME_MS,
    VERSION,
    WEB_PORT,
    WORKER_ADDRESSES,
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
