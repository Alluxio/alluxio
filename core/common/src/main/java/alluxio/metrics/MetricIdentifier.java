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

package alluxio.metrics;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The identifier to describe an unique metric.
 */
public final class MetricIdentifier implements Serializable {
  private final MetricsSystem.InstanceType mInstanceType;
  private final String mHostname;
  private final String mName;

  // TODO(yupeng): consider a dedicated data structure for tag, when more functionality are added to
  // tags in the future
  // Same metric with different tags also put in different metric
  private final Map<String, String> mTags;

  /**
   * Constructs a {@link MetricIdentifier} instance.
   *
   * @param instanceType the instance type
   * @param hostname the hostname
   * @param name the metric name
   */
  public MetricIdentifier(MetricsSystem.InstanceType instanceType, String hostname, String name) {
    Preconditions.checkNotNull(name, "name");
    mInstanceType = instanceType;
    mHostname = hostname;
    mName = name;
    mTags = new LinkedHashMap<>();
  }

  /**
   * @return the instance type
   */
  public MetricsSystem.InstanceType getInstanceType() {
    return mInstanceType;
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * @return the metric name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the tags
   */
  public Map<String, String> getTags() {
    return mTags;
  }

  /**
   * Adds a new tag. If the tag name already exists, it will be replaced.
   *
   * @param name the tag name
   * @param value the tag value
   */
  public void addTag(String name, String value) {
    mTags.put(name, value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetricIdentifier)) {
      return false;
    }
    MetricIdentifier  that = (MetricIdentifier) o;
    return Objects.equal(mInstanceType, that.mInstanceType)
        && Objects.equal(mHostname, that.mHostname)
        && mName.equals(that.mName)
        && Objects.equal(mTags, that.mTags);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mInstanceType, mHostname, mName, mTags);
  }
}
