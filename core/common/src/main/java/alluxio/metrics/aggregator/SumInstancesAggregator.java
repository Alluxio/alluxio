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
import alluxio.metrics.MetricsFilter;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.SingleValueAggregator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An aggregator that sums the metric values from all the metrics of a given instance type and
 * metric name. The aggregated metric will have name of pattern cluster.metric_name.
 */
public class SumInstancesAggregator implements SingleValueAggregator {
  private final MetricsSystem.InstanceType mInstanceType;
  private final String mAggregationName;
  private final MetricsFilter mFilter;

  /**
   * Creates an instance of {@link SumInstancesAggregator}.
   *
   * @param aggregationName the aggregation name
   * @param instanceType instance type which can be worker or client
   * @param metricName the metric name
   */
  public SumInstancesAggregator(String aggregationName, MetricsSystem.InstanceType instanceType,
      String metricName) {
    Preconditions.checkNotNull(instanceType, "instance type");
    Preconditions.checkNotNull(metricName, "metricName");
    Preconditions.checkNotNull(aggregationName, "aggregationName");
    mInstanceType = instanceType;
    mAggregationName = aggregationName;
    mFilter = new MetricsFilter(mInstanceType, metricName);
  }

  /**
   * @return the instance type to aggregate on
   */
  public MetricsSystem.InstanceType getInstanceType() {
    return mInstanceType;
  }

  @Override
  public String getName() {
    return mAggregationName;
  }

  @Override
  public List<MetricsFilter> getFilters() {
    return Lists.newArrayList(mFilter);
  }

  @Override
  public long getValue(Map<MetricsFilter, Set<Metric>> map) {
    long value = 0;
    for (Metric metric : map.get(mFilter)) {
      value += metric.getValue();
    }
    return value;
  }
}
