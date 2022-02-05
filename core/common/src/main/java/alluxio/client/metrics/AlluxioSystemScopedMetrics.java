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

import com.codahale.metrics.Meter;

import java.util.Set;

class AlluxioSystemScopedMetrics implements ScopedMetrics {

  @Override
  public Set<CacheScope> getAllCacheScopes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long inc(CacheScope scope, ScopedMetricKey scopedMetricKey, long n) {
    Meter meter = MetricsSystem.meter(getMetricsKey(scope, scopedMetricKey));
    meter.mark(n);
    return meter.getCount();
  }

  @Override
  public long getCount(CacheScope scope, ScopedMetricKey scopedMetricKey) {
    return MetricsSystem.meter(getMetricsKey(scope, scopedMetricKey)).getCount();
  }

  @Override
  public void switchOrClear() {
    throw new UnsupportedOperationException();
  }

  private String getMetricsKey(CacheScope scope, ScopedMetricKey key) {
    return key.getName() + "." + scope.id();
  }
}
