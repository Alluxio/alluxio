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

import java.util.Map;

/**
 * Alluxio job worker information.
 */
public final class AlluxioJobWorkerInfo {
  private Map<String, String> mConfiguration;
  private long mStartTimeMs;
  private long mUptimeMs;
  private String mVersion;

  /**
   * Creates a new instance of {@link AlluxioJobWorkerInfo}.
   */
  public AlluxioJobWorkerInfo() {}

  /**
   * @return the configuration
   */
  @ApiModelProperty(value = "Configuration of the Job Worker")
  public Map<String, String> getConfiguration() {
    return mConfiguration;
  }

  /**
   * @return the start time (in milliseconds)
   */
  @ApiModelProperty(value = "Job Worker's start time in epoch time")
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the uptime (in milliseconds)
   */
  @ApiModelProperty(value = "Number of milliseconds the Job Worker has been running")
  public long getUptimeMs() {
    return mUptimeMs;
  }

  /**
   * @return the version
   */
  @ApiModelProperty(value = "Version of the Job Worker")
  public String getVersion() {
    return mVersion;
  }

  /**
   * @param configuration the configuration to use
   * @return the Alluxio job worker information
   */
  public AlluxioJobWorkerInfo setConfiguration(Map<String, String> configuration) {
    mConfiguration = configuration;
    return this;
  }

  /**
   * @param startTimeMs the start time to use (in milliseconds)
   * @return the Alluxio job worker information
   */
  public AlluxioJobWorkerInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param uptimeMs the uptime to use (in milliseconds)
   * @return the Alluxio job worker information
   */
  public AlluxioJobWorkerInfo setUptimeMs(long uptimeMs) {
    mUptimeMs = uptimeMs;
    return this;
  }

  /**
   * @param version the version to use
   * @return the Alluxio job worker information
   */
  public AlluxioJobWorkerInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlluxioJobWorkerInfo)) {
      return false;
    }
    AlluxioJobWorkerInfo that = (AlluxioJobWorkerInfo) o;
    return Objects.equal(mConfiguration, that.mConfiguration)
        && mStartTimeMs == that.mStartTimeMs
        && mUptimeMs == that.mUptimeMs
        && Objects.equal(mVersion, that.mVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mConfiguration, mStartTimeMs, mUptimeMs, mVersion);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("configuration", mConfiguration)
        .add("start time", mStartTimeMs)
        .add("uptime", mUptimeMs)
        .add("version", mVersion)
        .toString();
  }
}
