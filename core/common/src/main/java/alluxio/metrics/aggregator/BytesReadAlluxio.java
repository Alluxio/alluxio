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

package alluxio.metrics.aggregator;

import alluxio.metrics.Metric;
import alluxio.metrics.MetricsAggregator;
import alluxio.metrics.MetricsFilter;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Metrics of aggregating all the bytes read from Alluxio from the cluster.
 */
public final class BytesReadAlluxio implements MetricsAggregator {
  public static final String NAME = "BytesReadAlluxio";
  public static final MetricsFilter BYTES_READ_ALLUXIO_FILTER =
      new MetricsFilter(MetricsSystem.WORKER_INSTANCE, WorkerMetrics.BYTES_READ_ALLUXIO);

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<MetricsFilter> getFilters() {
    return Lists.newArrayList(BYTES_READ_ALLUXIO_FILTER);
  }

  @Override
  public Object getValue(Map<MetricsFilter, Set<Metric>> map) {
    long value = 0;
    for (Metric metric : map.get(BYTES_READ_ALLUXIO_FILTER)) {
      value += (long) metric.getValue();
    }
    return value;
  }
}
