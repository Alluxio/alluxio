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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio master information.
 */
@NotThreadSafe
public class AlluxioMasterInfo {
  private Capacity mCapacity;
  private Map<String, String> mConfiguration;
  private List<WorkerInfo> mLostWorkers;
  private Map<String, Long> mMetrics;
  private Map<String, MountPointInfo> mMountPoints;
  private String mRpcAddress;
  private long mStartTimeMs;
  private Map<String, Capacity> mTierCapacity;
  private Capacity mUfsCapacity;
  private long mUptimeMs;
  private String mVersion;
  private List<WorkerInfo> mWorkers;

  /**
   * Creates a new instance of {@link AlluxioMasterInfo}.
   */
  public AlluxioMasterInfo() {}

  /**
   * @return the capacity
   */
  public Capacity getCapacity() {
    return mCapacity;
  }

  /**
   * @return the configuration
   */
  public Map<String, String> getConfiguration() {
    return mConfiguration;
  }

  /**
   * @return the list of lost workers
   */
  public List<WorkerInfo> getLostWorkers() {
    return mLostWorkers;
  }

  /**
   * @return the metrics
   */
  public Map<String, Long> getMetrics() {
    return mMetrics;
  }

  /**
   * @return the mount points
   */
  public Map<String, MountPointInfo> getMountPoints() {
    return mMountPoints;
  }

  /**
   * @return the RPC address
   */
  public String getRpcAddress() {
    return mRpcAddress;
  }

  /**
   * @return the start time (in milliseconds)
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the capacity per tier
   */
  public Map<String, Capacity> getTierCapacity() {
    return mTierCapacity;
  }

  /**
   * @return the UFS capacity
   */
  public Capacity getUfsCapacity() {
    return mUfsCapacity;
  }

  /**
   * @return the uptime (in milliseconds)
   */
  public long getUptimeMs() {
    return mUptimeMs;
  }

  /**
   * @return the version
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * @return the list of workers
   */
  public List<WorkerInfo> getWorkers() {
    return mWorkers;
  }

  /**
   * @param capacity the capacity to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setCapacity(Capacity capacity) {
    mCapacity = capacity;
    return this;
  }

  /**
   * @param configuration the configuration to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setConfiguration(Map<String, String> configuration) {
    mConfiguration = configuration;
    return this;
  }

  /**
   * @param lostWorkers the list of lost workers to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setLostWorkers(List<WorkerInfo> lostWorkers) {
    mLostWorkers = lostWorkers;
    return this;
  }

  /**
   * @param metrics the metrics to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setMetrics(Map<String, Long> metrics) {
    mMetrics = metrics;
    return this;
  }

  /**
   * @param mountPoints the mount points to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setMountPoints(Map<String, MountPointInfo> mountPoints) {
    mMountPoints = mountPoints;
    return this;
  }

  /**
   * @param rpcAddress the RPC address to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setRpcAddress(String rpcAddress) {
    mRpcAddress = rpcAddress;
    return this;
  }

  /**
   * @param startTimeMs the start time to use (in milliseconds)
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param tierCapacity the capacity per tier to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setTierCapacity(Map<String, Capacity> tierCapacity) {
    mTierCapacity = tierCapacity;
    return this;
  }

  /**
   * @param ufsCapacity the UFS capacity to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setUfsCapacity(Capacity ufsCapacity) {
    mUfsCapacity = ufsCapacity;
    return this;
  }

  /**
   * @param uptimeMs the uptime to use (in milliseconds)
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setUptimeMs(long uptimeMs) {
    mUptimeMs = uptimeMs;
    return this;
  }

  /**
   * @param version the version to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * @param workers the list of workers to use
   * @return the Alluxio master information
   */
  public AlluxioMasterInfo setWorkers(List<WorkerInfo> workers) {
    mWorkers = workers;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlluxioMasterInfo)) {
      return false;
    }
    AlluxioMasterInfo that = (AlluxioMasterInfo) o;
    return Objects.equal(mCapacity, that.mCapacity)
        && Objects.equal(mConfiguration, that.mConfiguration)
        && Objects.equal(mLostWorkers, that.mLostWorkers)
        && Objects.equal(mMetrics, that.mMetrics)
        && Objects.equal(mMountPoints, that.mMountPoints)
        && Objects.equal(mRpcAddress, that.mRpcAddress)
        && mStartTimeMs == that.mStartTimeMs
        && Objects.equal(mTierCapacity, that.mTierCapacity)
        && Objects.equal(mUfsCapacity, that.mUfsCapacity)
        && mUptimeMs == that.mUptimeMs
        && Objects.equal(mVersion, that.mVersion)
        && Objects.equal(mWorkers, that.mWorkers);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mCapacity, mConfiguration, mLostWorkers, mMetrics, mMountPoints, mRpcAddress,
            mStartTimeMs, mTierCapacity, mUfsCapacity, mUptimeMs,
            mVersion, mWorkers);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("capacity", mCapacity)
        .add("configuration", mConfiguration)
        .add("lost workers", mLostWorkers)
        .add("metrics", mMetrics)
        .add("mount points", mMountPoints)
        .add("rpc address", mRpcAddress)
        .add("start time", mStartTimeMs)
        .add("tier capacity", mTierCapacity)
        .add("ufs capacity", mUfsCapacity)
        .add("uptime", mUptimeMs)
        .add("version", mVersion)
        .add("workers", mWorkers)
        .toString();
  }
}
