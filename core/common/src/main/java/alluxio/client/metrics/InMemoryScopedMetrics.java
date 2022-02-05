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

package alluxio.client.metrics;

import alluxio.client.quota.CacheScope;
import alluxio.metrics.MetricsSystem;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class InMemoryScopedMetrics implements ScopedMetrics {

  Map<CacheScope, MetricItem> mMetrics;

  InMemoryScopedMetrics() {
    mMetrics = new ConcurrentHashMap<>();
  }

  @Override
  public Set<CacheScope> getAllCacheScopes() {
    return Collections.unmodifiableSet(mMetrics.keySet());
  }

  @Override
  public long inc(CacheScope scope, ScopedMetricKey scopedMetricKey, long n) {
    MetricsSystem.meter(scopedMetricKey.getName() + "." + scope.toString()).mark(n);
    return getMetricItem(scope).inc(scopedMetricKey, n);
  }

  @Override
  public long getCount(CacheScope scope, ScopedMetricKey scopedMetricKey) {
    return getMetricItem(scope).getCount(scopedMetricKey);
  }

  @Override
  public void switchOrClear() {
    mMetrics.clear();
  }

  private MetricItem getMetricItem(CacheScope scope) {
    return mMetrics
        .computeIfAbsent(scope, k -> new MetricItem());
  }
}
