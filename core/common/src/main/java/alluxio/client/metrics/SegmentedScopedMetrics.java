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

import java.util.HashSet;
import java.util.Set;

/**
 * Provide an aggregated view for segmented metrics such as the metrics for shadow cache.
 */
public class SegmentedScopedMetrics implements ScopedMetrics {

  private final int mNumOfSegments;
  private final ScopedMetrics[] mSegmentedMetrics;
  private int mCurrentSegmentIndex;

  SegmentedScopedMetrics(int numOfSegments) {
    mNumOfSegments = numOfSegments;
    mSegmentedMetrics = new ScopedMetrics[numOfSegments];
    for (int i = 0; i < numOfSegments; i++) {
      mSegmentedMetrics[i] = new ConcurrentScopedMetrics();
    }
  }

  @Override
  public void switchOrClear() {
    int newSegmentIndex = (mCurrentSegmentIndex + 1) % mNumOfSegments;
    mSegmentedMetrics[newSegmentIndex].switchOrClear();
    mCurrentSegmentIndex = newSegmentIndex;
  }

  @Override
  public Set<CacheScope> getAllCacheScopes() {
    Set<CacheScope> metricsKeys = new HashSet<>();
    for (int i = 0; i < mNumOfSegments; i++) {
      metricsKeys.addAll(mSegmentedMetrics[i].getAllCacheScopes());
    }
    return metricsKeys;
  }

  @Override
  public long inc(CacheScope scope, ScopedMetricKey scopedMetricKey, long n) {
    return mSegmentedMetrics[mCurrentSegmentIndex].inc(scope, scopedMetricKey, n);
  }

  @Override
  public long getCount(CacheScope scope, ScopedMetricKey scopedMetricKey) {
    long sum = 0;
    for (int i = 0; i < mNumOfSegments; i++) {
      sum += mSegmentedMetrics[i].getCount(scope, scopedMetricKey);
    }
    return sum;
  }
}
