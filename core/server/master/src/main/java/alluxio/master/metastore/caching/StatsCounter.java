package alluxio.master.metastore.caching;

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Counter;

/**
 * Implementation of StatsCounter similar to the one in
 * {@link com.google.common.cache.AbstractCache}.
 */
final class StatsCounter {
  private final Counter mHitCount;
  private final Counter mMissCount;
  private final Counter mTotalLoadTime;
  private final Counter mEvictionCount;

  public StatsCounter(MetricKey evictionsKey, MetricKey hitsKey, MetricKey loadTimesKey,
                      MetricKey missesKey) {
    mHitCount = MetricsSystem.counter(hitsKey.getName());
    mMissCount = MetricsSystem.counter(missesKey.getName());
    mTotalLoadTime = MetricsSystem.counter(loadTimesKey.getName());
    mEvictionCount = MetricsSystem.counter(evictionsKey.getName());
  }

  /**
   * Record a cache hit.
   */
  public void recordHit() {
    mHitCount.inc();
  }

  /**
   * Record a cache miss.
   */
  public void recordMiss() {
    mMissCount.inc();
  }

  /**
   * Record a loading of data asa result of cache miss.
   * @param loadTime amount of time it took to load data in nanoseconds
   */
  public void recordLoad(long loadTime) {
    mTotalLoadTime.inc(loadTime);
  }

  /**
   * Record evictions in the cache.
   * @param evictionCount the number of evictions
   */
  public void recordEvictions(long evictionCount) {
    mEvictionCount.inc(evictionCount);
  }
}
