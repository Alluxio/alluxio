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

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio proxy information.
 */
@NotThreadSafe
public class AlluxioProxyInfo {
  private Map<String, String> mConfiguration;
  private long mStartTimeMs;
  private long mUptimeMs;
  private String mVersion;

  /**
   * Creates a new instance of {@link AlluxioProxyInfo}.
   */
  public AlluxioProxyInfo() {}

  /**
   * @return the configuration
   */
  public Map<String, String> getConfiguration() {
    return mConfiguration;
  }

  /**
   * @return the start time (in milliseconds)
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
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
   * @param configuration the configuration to use
   * @return the Alluxio proxy information
   */
  public AlluxioProxyInfo setConfiguration(Map<String, String> configuration) {
    mConfiguration = configuration;
    return this;
  }

  /**
   * @param startTimeMs the start time to use (in milliseconds)
   * @return the Alluxio proxy information
   */
  public AlluxioProxyInfo setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
    return this;
  }

  /**
   * @param uptimeMs the uptime to use (in milliseconds)
   * @return the Alluxio proxy information
   */
  public AlluxioProxyInfo setUptimeMs(long uptimeMs) {
    mUptimeMs = uptimeMs;
    return this;
  }

  /**
   * @param version the version to use
   * @return the Alluxio proxy information
   */
  public AlluxioProxyInfo setVersion(String version) {
    mVersion = version;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlluxioProxyInfo)) {
      return false;
    }
    AlluxioProxyInfo that = (AlluxioProxyInfo) o;
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
