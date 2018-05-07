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

/**
 * A metric of a given instance. The instance can be master, worker, or client.
 */
public final class Metric implements Serializable {
  private static final long serialVersionUID = -2236393414222298333L;

  private final MetricsSystem.InstanceType mInstance;
  private final String mHostname;
  private final String mName;
  private final Long mValue;

  /**
   * Constructs a {@link Metric} instance.
   *
   * @param instance the instance
   * @param hostname the hostname
   * @param name the metric name
   * @param value the value
   */
  public Metric(MetricsSystem.InstanceType instance, String hostname, String name, Long value) {
    Preconditions.checkNotNull(name);
    mInstance = instance;
    mHostname = hostname;
    mName = name;
    mValue = value;
  }

  /**
   * @return the instance type
   */
  public MetricsSystem.InstanceType getInstance() {
    return mInstance;
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
   * @return the metric value
   */
  public long getValue() {
    return mValue;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof Metric)) {
      return false;
    }
    Metric metric = (Metric) other;
    return Objects.equal(mHostname, metric.mHostname) && Objects.equal(mInstance, metric.mInstance)
        && Objects.equal(mName, metric.mName) && Objects.equal(mValue, metric.mValue);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mHostname, mInstance, mValue, mName);
  }

  /**
   * @return the fully qualified metric name, which is of pattern instance.[hostname.].value
   */
  public String getFullMetricName() {
    StringBuilder sb = new StringBuilder();
    sb.append(mInstance).append('.');
    if (mHostname != null) {
      sb.append(mHostname).append('.');
    }
    sb.append(mName);
    return sb.toString();
  }

  /**
   * @return the thrift object it converts to. Note the value must be either integer or long
   */
  public alluxio.thrift.Metric toThrift() {
    alluxio.thrift.Metric metric = new alluxio.thrift.Metric();
    metric.setInstance(mInstance.toString());
    metric.setHostname(mHostname);
    metric.setName(mName);
    metric.setValue(mValue);
    return metric;
  }

  /**
   * Creates the metric from the full name and the value.
   *
   * @param fullName the full name
   * @param value the value
   * @return the created metric
   */
  public static Metric from(String fullName, long value) {
    String[] pieces = fullName.split("\\.");
    Preconditions.checkArgument(pieces.length > 1, "Incorrect metrics name: %s.", fullName);

    String hostname = null;
    // Master or cluster metrics don't have hostname included.
    if (!pieces[0].equals(MetricsSystem.InstanceType.MASTER.toString())) {
      hostname = pieces[1];
    }
    MetricsSystem.InstanceType instance = MetricsSystem.InstanceType.fromString(pieces[0]);
    String name = MetricsSystem.stripInstanceAndHost(fullName);
    return new Metric(instance, hostname, name, value);
  }

  /**
   * Constructs the metric object from the thrift format.
   *
   * @param metric the metric in thrift format
   * @return the constructed metric
   */
  public static Metric from(alluxio.thrift.Metric metric) {
    return new Metric(MetricsSystem.InstanceType.fromString(metric.getInstance()),
        metric.getHostname(), metric.getName(), metric.getValue());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("instance", mInstance).add("hostname", mHostname)
        .add("name", mName).add("value", mValue).toString();
  }
}
