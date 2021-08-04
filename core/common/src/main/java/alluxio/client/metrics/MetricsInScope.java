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

import java.util.Set;

/**
 * Provide a map-like interface to keep and collect the metrics of each CacheScope.
 */
public interface MetricsInScope {

  /**
   * Get the set of all the scopes.
   * @return the set of all the scopes
   */
  Set<CacheScope> getAllCacheScopes();

  /**
   * Increment the counter of the given scope by n.
   * @param scope the scope of the metrics
   * @param metricKeyInScope the key of the metrics
   * @param n the amount by which the counter will be increased
   * @return the new value of the counter
   */
  long inc(CacheScope scope, MetricKeyInScope metricKeyInScope, long n);

  /**
   * Decrement the counter of the given scope by n.
   * @param scope the scope of the metrics
   * @param metricKeyInScope the key of the metrics
   * @param n the amount by which the counter will be decreased
   * @return the new value of the counter
   */
  default long dec(CacheScope scope, MetricKeyInScope metricKeyInScope, long n) {
    return inc(scope, metricKeyInScope, -n);
  }

  /**
   * Returns current value of the given scope.
   * @param scope the scope of the metrics
   * @param metricKeyInScope the key of the metrics
   * @return current value of the given scope
   */
  long getCount(CacheScope scope, MetricKeyInScope metricKeyInScope);

  /**
   * If the metrics are segmented, remove the oldest segment and create a new one.
   * If not, removes all metrics stored. The metrics will be empty after this call returns.
   */
  void switchOrClear();
}

