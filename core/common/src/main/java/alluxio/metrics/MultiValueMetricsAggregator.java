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

import java.util.Map;
import java.util.Set;

/**
 * An interface for the aggregator that aggregates metrics of different hosts into multiple
 * cluster-level metrics.
 */
public interface MultiValueMetricsAggregator {
  /**
   * @return the metric name to use for filter metrics
   */
  String getFilterMetricName();

  /**
   * Updates the aggregated values from the filtered metrics. The values of map will be the filtered
   * metrics using the metric name defined in {@link #getFilterMetricName()}. The returned values
   * are organized as a map from the metric name to metric value.
   *
   * @param map a map of metric name to the set of metrics that have the metric name
   * @return the aggregated values
   */
  Map<String, Long> updateValues(Set<Metric> map);

  /**
   * Gets the metric value of the given fully qualified metric name.
   *
   * @param name the fully qualified metric name
   * @return the metric value, 0 if the metric name does not exist
   */
  long getValue(String name);
}
