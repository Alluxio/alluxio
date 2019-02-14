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

package alluxio.master.metrics;

import alluxio.metrics.TimeSeries;

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory time series store for Alluxio metrics.
 */
@ThreadSafe
public class TimeSeriesStore {
  /** <MetricName, TimeSeries>. **/
  private final Map<String, TimeSeries> mTimeSeries;

  /**
   * Constructs a new time series store.
   */
  public TimeSeriesStore() {
    mTimeSeries = new ConcurrentHashMap<>();
  }

  /**
   * Records a value for the given metric at the current time.
   *
   * @param metric the name of the metric
   * @param value the value of the metric
   */
  public void record(String metric, double value) {
    mTimeSeries.compute(metric, (metricName, timeSeries) -> {
      if (timeSeries == null) {
        timeSeries = new TimeSeries(metricName);
      }
      timeSeries.record(value);
      return timeSeries;
    });
  }

  /**
   * @return a copy of all the time series data collected so far
   */
  public List<TimeSeries> getTimeSeries() {
    return ImmutableList.copyOf(mTimeSeries.values());
  }
}
