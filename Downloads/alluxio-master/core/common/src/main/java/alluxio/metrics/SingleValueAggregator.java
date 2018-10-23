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
 * Interface for the aggregator that aggregates into a single value from the metrics of different
 * hosts.
 */
public interface SingleValueAggregator extends MetricsAggregator {

  /**
   * Gets the aggregated value from the filtered metrics. The values of map will be the filtered
   * metrics using the {@link MetricsFilter} defined in {@link #getFilters()}.
   *
   * @param map a map of {@link MetricsFilter} to the set of metrics that it filter to
   * @return the aggregated value
   */
  long getValue(Map<MetricsFilter, Set<Metric>> map);
}
