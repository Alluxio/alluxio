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

import java.util.Collections;
import java.util.Set;

/**
 * No operation metrics collector.
 */
class NoOpMetricsInScope implements MetricsInScope {

  @Override
  public Set<CacheScope> getAllCacheScopes() {
    return Collections.emptySet();
  }

  @Override
  public long inc(CacheScope scope, MetricKeyInScope metricKeyInScope, long n) {
    return 0;
  }

  @Override
  public long getCount(CacheScope scope, MetricKeyInScope metricKeyInScope) {
    return 0;
  }

  @Override
  public void switchOrClear() {
  }
}
