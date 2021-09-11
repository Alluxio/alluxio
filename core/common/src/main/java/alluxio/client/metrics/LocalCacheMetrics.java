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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

/**
 * A singleton class to hold and expose local cache metrics.
 */
public class LocalCacheMetrics {

  private ScopedMetrics mLocalCacheScopedMetrics;

  private ScopedMetrics mShadowCacheScopedMetrics;

  private LocalCacheMetrics() {
  }

  /**
   * Expose the metrics breakdown by scope for local cache.
   * @return metrics breakdown for local cache
   */
  public ScopedMetrics getLocalCacheMetricsInScope() {
    return mLocalCacheScopedMetrics;
  }

  private void setLocalCacheMetricsInScope(ScopedMetrics localCacheScopedMetrics) {
    mLocalCacheScopedMetrics = localCacheScopedMetrics;
  }

  /**
   * Expose the metrics breakdown by scope for shadow cache.
   * @return metrics breakdown for shadow cache
   */
  public ScopedMetrics getShadowCacheMetricsInScope() {
    return mShadowCacheScopedMetrics;
  }

  private void setShadowCacheMetricsInScope(ScopedMetrics shadowCacheScopedMetrics) {
    mShadowCacheScopedMetrics = shadowCacheScopedMetrics;
  }

  /**
   * Factory of LocalCacheMetrics.
   */
  public static class Factory {
    private static LocalCacheMetrics sMetrics;

    /**
     * @param conf AlluxioConfiguration
     * @return the singleton instance of LocalCacheMetrics
     */
    public static LocalCacheMetrics get(AlluxioConfiguration conf) {
      if (sMetrics == null) {
        synchronized (LocalCacheMetrics.Factory.class) {
          if (sMetrics == null) {
            sMetrics = create(conf);
          }
        }
      }
      return sMetrics;
    }

    private static LocalCacheMetrics create(AlluxioConfiguration conf) {
      LocalCacheMetrics metrics = new LocalCacheMetrics();
      if (conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED)
          || conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_METRICS_BREAKDOWN_ENABLED)) {
        metrics.setLocalCacheMetricsInScope(new ConcurrentScopedMetrics());
      } else {
        metrics.setLocalCacheMetricsInScope(new NoOpScopedMetrics());
      }
      if (conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_SHADOW_METRICS_BREAKDOWN_ENABLED)) {
        int numOfSegments = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_BLOOMFILTER_NUM);
        metrics.setShadowCacheMetricsInScope(new SegmentedScopedMetrics(numOfSegments));
      } else {
        metrics.setShadowCacheMetricsInScope(new NoOpScopedMetrics());
      }
      return metrics;
    }
  }
}
