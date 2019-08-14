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
import io.swagger.annotations.ApiModelProperty;

import java.util.List;
import java.util.Map;

/**
 * Alluxio job master information.
 */
public final class AlluxioJobMasterInfo {
  private Map<String, String> mConfiguration;
  private long mStartTimeMs;
  private long mUptimeMs;
  private String mVersion;
  private List<WorkerInfo> mWorkers;

  /**
   * Creates a new instance of {@link AlluxioJobMasterInfo}.
   */
  public AlluxioJobMasterInfo() {}

  /**
   * @return the configuration
   */
  @ApiModelProperty(value = "Configuration of the Job Master")
  public Map<String, String> getConfiguration() {
    return mConfiguration;
  }

  /**
   * @return the start time (in milliseconds)
   */
  @ApiModelProperty(value = "Job Master's start time in epoch time")
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the uptime (in milliseconds)
   */
  @ApiModelProperty(value = "Number of milliseconds the Job Master has been running")
  public long getUptimeMs() {
    return mUptimeMs;
  }

  /**
   * @return the version
   */
  @ApiModelProperty(value = "Version of the Job Master")
  public String getVersion() {
    return mVersion;
  }

  /**
   * @return the list of workers
   */
  @ApiModelProperty(value = "List of Job Workers that have registered with the Job Master")
  public List<WorkerInfo> getWorkers() {
    return mWorkers;
  }

  /**
   * @param configuration the configuration to use
   * @return the Alluxio job master information
   */
  public AlluxioJobMasterInfo setConfiguration(Map<String, String> configuration) {
    mConfiguration = configuration;
    return this;
  }

  /**
   * @param startTimeMs the start time to use (in milliseconds)
   * @return the Alluxio job master information
   */
  public AlluxioJobMasterInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param uptimeMs the uptime to use (in milliseconds)
   * @return the Alluxio job master information
   */
  public AlluxioJobMasterInfo setUptimeMs(long uptimeMs) {
    mUptimeMs = uptimeMs;
    return this;
  }

  /**
   * @param version the version to use
   * @return the Alluxio job master information
   */
  public AlluxioJobMasterInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * @param workers the list of workers to use
   * @return the Alluxio master information
   */
  public AlluxioJobMasterInfo setWorkers(List<WorkerInfo> workers) {
    mWorkers = workers;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlluxioJobMasterInfo)) {
      return false;
    }
    AlluxioJobMasterInfo that = (AlluxioJobMasterInfo) o;
    return Objects.equal(mConfiguration, that.mConfiguration)
        && mStartTimeMs == that.mStartTimeMs
        && mUptimeMs == that.mUptimeMs
        && Objects.equal(mVersion, that.mVersion)
        && Objects.equal(mWorkers, that.mWorkers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mConfiguration, mStartTimeMs, mUptimeMs, mVersion, mWorkers);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("configuration", mConfiguration)
        .add("start time", mStartTimeMs)
        .add("uptime", mUptimeMs)
        .add("version", mVersion)
        .add("workers", mWorkers)
        .toString();
  }
}
