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
 * Alluxio worker information.
 */
@NotThreadSafe
public class AlluxioWorkerInfo {
  private Capacity mCapacity;
  private Map<String, String> mConfiguration;
  private Map<String, Long> mMetrics;
  private String mRpcAddress;
  private long mStartTimeMs;
  private Map<String, Capacity> mTierCapacity;
  private Map<String, List<String>> mTierPaths;
  private long mUptimeMs;
  private String mVersion;

  /**
   * Creates a new instance of {@link AlluxioWorkerInfo}.
   */
  public AlluxioWorkerInfo() {}

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
   * @return the metrics
   */
  public Map<String, Long> getMetrics() {
    return mMetrics;
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
   * @return the tier paths
   */
  public Map<String, List<String>> getTierPaths() {
    return mTierPaths;
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
   * @param capacity the capacity to use
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setCapacity(Capacity capacity) {
    mCapacity = capacity;
    return this;
  }

  /**
   * @param configuration the configuration to use
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setConfiguration(Map<String, String> configuration) {
    mConfiguration = configuration;
    return this;
  }

  /**
   * @param metrics the metrics to use
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setMetrics(Map<String, Long> metrics) {
    mMetrics = metrics;
    return this;
  }

  /**
   * @param rpcAddress the RPC address to use
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setRpcAddress(String rpcAddress) {
    mRpcAddress = rpcAddress;
    return this;
  }

  /**
   * @param startTimeMs the start time to use (in milliseconds)
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param tierCapacity the capacity per tier to use
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setTierCapacity(Map<String, Capacity> tierCapacity) {
    mTierCapacity = tierCapacity;
    return this;
  }

  /**
   * @param tierPaths the tier paths to use
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setTierPaths(Map<String, List<String>> tierPaths) {
    mTierPaths = tierPaths;
    return this;
  }

  /**
   * @param uptimeMs the uptime to use (in milliseconds)
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setUptimeMs(long uptimeMs) {
    mUptimeMs = uptimeMs;
    return this;
  }

  /**
   * @param version the version to use
   * @return the Alluxio worker information
   */
  public AlluxioWorkerInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlluxioWorkerInfo)) {
      return false;
    }
    AlluxioWorkerInfo that = (AlluxioWorkerInfo) o;
    return Objects.equal(mCapacity, that.mCapacity)
        && Objects.equal(mConfiguration, that.mConfiguration)
        && Objects.equal(mMetrics, that.mMetrics)
        && Objects.equal(mRpcAddress, that.mRpcAddress)
        && mStartTimeMs == that.mStartTimeMs
        && Objects.equal(mTierCapacity, that.mTierCapacity)
        && Objects.equal(mTierPaths, that.mTierPaths)
        && mUptimeMs == that.mUptimeMs
        && Objects.equal(mVersion, that.mVersion);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mCapacity, mConfiguration, mMetrics, mRpcAddress, mStartTimeMs, mTierCapacity,
            mTierPaths, mUptimeMs, mVersion);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("capacity", mCapacity)
        .add("configuration", mConfiguration)
        .add("metrics", mMetrics)
        .add("rpc address", mRpcAddress)
        .add("start time", mStartTimeMs)
        .add("tier capacity", mTierCapacity)
        .add("tier paths", mTierPaths)
        .add("uptime", mUptimeMs)
        .add("version", mVersion).toString();
  }
}
