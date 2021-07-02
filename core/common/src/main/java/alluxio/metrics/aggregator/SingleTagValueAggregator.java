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
import alluxio.metrics.MultiValueMetricsAggregator;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An aggregator that aggregates the metrics into multiple values based on a single tag of the
 * metric.
 */
public class SingleTagValueAggregator implements MultiValueMetricsAggregator {
  private final String mAggregationName;
  private final String mMetricName;
  private final String mTagName;
  /** Cached aggregated metric values. */
  private Map<String, Long> mAggregates;

  /**
   * Constructs a new instance of {@link SingleTagValueAggregator}.
   *
   * @param aggregationName the aggregated metric name
   * @param metricName metric name
   * @param tagName tag name
   */
  public SingleTagValueAggregator(String aggregationName,
      String metricName, String tagName) {
    Preconditions.checkNotNull(aggregationName, "aggregationName");
    Preconditions.checkNotNull(metricName, "metricName");
    Preconditions.checkNotNull(tagName, "tagName");
    mAggregationName = aggregationName;
    mMetricName = metricName;
    mTagName = tagName;
    mAggregates = new HashMap<>();
  }

  @Override
  public String getFilterMetricName() {
    return mMetricName;
  }

  @Override
  public Map<String, Long> updateValues(Set<Metric> set) {
    Map<String, Long> updated = new HashMap<>();
    for (Metric metric : set) {
      Map<String, String> tags = metric.getTags();
      if (tags.containsKey(mTagName)) {
        String ufsName =
            Metric.getMetricNameWithTags(mAggregationName, mTagName, tags.get(mTagName));
        long value = updated.getOrDefault(ufsName, 0L);
        updated.put(ufsName, (long) (value + metric.getValue()));
      }
    }
    synchronized (this) {
      mAggregates = updated;
    }
    return Collections.unmodifiableMap(mAggregates);
  }

  @Override
  public long getValue(String name) {
    return mAggregates.getOrDefault(name, 0L);
  }
}
