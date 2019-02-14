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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * A filter that matches the instance-level metrics by the instance type and metric name.
 */
public class MetricsFilter {
  private final MetricsSystem.InstanceType mInstanceType;
  private final String mName;

  /**
   * Constructs a new {@link MetricsFilter} instance.
   *
   * @param instanceType the instance type of the instance-level metric
   * @param name the metric name
   */
  public MetricsFilter(MetricsSystem.InstanceType instanceType, String name) {
    mInstanceType = instanceType;
    mName = name;
  }

  /**
   * @return the instance type
   */
  public MetricsSystem.InstanceType getInstanceType() {
    return mInstanceType;
  }

  /**
   * @return the metric name
   */
  public String getName() {
    return mName;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof MetricsFilter)) {
      return false;
    }
    MetricsFilter filter = (MetricsFilter) obj;
    return Objects.equal(mInstanceType, filter.mInstanceType) && Objects.equal(mName, filter.mName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mInstanceType, mName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("instanceType", mInstanceType).add("name", mName)
        .toString();
  }
}
