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

import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory time series store for Alluxio metrics.
 */
@ThreadSafe
public class TimeSeriesStore {
  /** <MetricName, Map<Epoch Time, Value>>. **/
  private final Map<String, TreeMap<Long, Long>> mTimeSeries;

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
  public void record(String metric, long value) {
    mTimeSeries.compute(metric, (metricName, timeseries) -> {
      if (timeseries == null) {
        timeseries = new TreeMap<>();
      }
      timeseries.put(System.currentTimeMillis(), value);
      return timeseries;
    });
  }

  /**
   * @return a copy of the time series data collected so far
   */
  public Map<String, TreeMap<Long, Long>> getTimeSeries() {
    return ImmutableMap.copyOf(mTimeSeries);
  }
}
