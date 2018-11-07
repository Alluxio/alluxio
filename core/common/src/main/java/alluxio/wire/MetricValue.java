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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents the value of a metrics property, only one of longValue or doubleValue should be set.
 */
@NotThreadSafe
public final class MetricValue {

  private Double mDoubleValue;
  private Long mLongValue;

  /**
   * Creates a new instance of {@link MetricValue} from a thrift representation.
   *
   * @param metricValue the thrift representation of a metric value
   */
  private MetricValue(alluxio.thrift.MetricValue metricValue) {
    Preconditions.checkState(metricValue.isSetDoubleValue() || metricValue.isSetLongValue(),
        "only one of longValue and doubleValue can be set");
    if (metricValue.isSetDoubleValue()) {
      mDoubleValue = metricValue.getDoubleValue();
    } else if (metricValue.isSetLongValue()) {
      mLongValue = metricValue.getLongValue();
    }
  }

  @JsonCreator
  private MetricValue(
      @JsonProperty("doubleValue") Double doubleValue,
      @JsonProperty("longValue") Long longValue) {
    Preconditions.checkState(doubleValue == null || longValue == null,
        "only one of longValue and doubleValue can be set");
    mDoubleValue = doubleValue;
    mLongValue = longValue;
  }

  /**
   * @param doubleValue the double value to set
   * @return a new metric value with double value
   */
  public static MetricValue forDouble(double doubleValue) {
    return new MetricValue(doubleValue, null);
  }

  /**
   * @param longValue the long value to set
   * @return a new metric value with long value
   */
  public static MetricValue forLong(long longValue) {
    return new MetricValue(null, longValue);
  }

  /**
   * @return the Double value
   */
  public Double getDoubleValue() {
    return mDoubleValue;
  }

  /**
   * @return the Long value
   */
  public Long getLongValue() {
    return mLongValue;
  }

  /**
   * @return thrift representation of the metric value
   */
  public alluxio.thrift.MetricValue toThrift() {
    if (mDoubleValue != null) {
      return new alluxio.thrift.MetricValue().setDoubleValue(mDoubleValue);
    } else {
      return new alluxio.thrift.MetricValue().setLongValue(mLongValue);
    }
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
