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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the value of a metrics property, only one of longValue or doubleValue should be set.
 */
@NotThreadSafe
public final class MetricValue {

  private double mDoubleValue;
  private long mLongValue;

  /**
   * Creates a new instance of {@link MetricValue}.
   */
  public MetricValue() {}

  /**
   * Creates a new instance of {@link MetricValue} from a thrift representation.
   *
   * @param metricValue the thrift representation of a metric value
   */
  private MetricValue(alluxio.thrift.MetricValue metricValue) {
    mDoubleValue = metricValue.getDoubleValue();
    mLongValue = metricValue.getLongValue();
  }

  /**
   * @return the double value
   */
  public double getDoubleValue() {
    return mDoubleValue;
  }

  /**
   * @return the long value
   */
  public long getLongValue() {
    return mLongValue;
  }

  /**
   * @param doubleValue the double value to set
   * @return the updated metric value
   */
  public MetricValue setDoubleValue(double doubleValue) {
    mDoubleValue = doubleValue;
    return this;
  }

  /**
   * @param longValue the long value to set
   * @return the updated metric value
   */
  public MetricValue setLongValue(long longValue) {
    mLongValue = longValue;
    return this;
  }

  /**
   * @return thrift representation of the metric value
   */
  public alluxio.thrift.MetricValue toThrift() {
    return new alluxio.thrift.MetricValue().setDoubleValue(mDoubleValue).setLongValue(mLongValue);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param metricValue the thrift representation of a metric value
   * @return the wire type metric value
   */
  public static MetricValue fromThrift(alluxio.thrift.MetricValue metricValue) {
    return new MetricValue(metricValue);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("doubleValue", mDoubleValue)
        .add("longValue", mLongValue).toString();
  }
}
