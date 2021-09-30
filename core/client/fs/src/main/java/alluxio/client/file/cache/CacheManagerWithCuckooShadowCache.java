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

package alluxio.client.file.cache;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.filter.ConcurrentClockCuckooFilter;
import alluxio.client.file.cache.filter.ScopeInfo;
import alluxio.client.file.cache.filter.SlidingWindowType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A wrapper class of CacheManager with shadow cache.
 */
public class CacheManagerWithCuckooShadowCache implements CacheManager {
  private final CacheManager mCacheManager;
  private final AtomicLong mShadowCachePageRead = new AtomicLong(0);
  private final AtomicLong mShadowCachePageHit = new AtomicLong(0);
  private final ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
  private final AtomicLong mShadowCacheByteRead = new AtomicLong(0);
  private long mShadowCacheBytes = 0;
  private final AtomicLong mShadowCacheByteHit = new AtomicLong(0);
  private long mShadowCachePages = 0;

  private ScopeInfo scope = new ScopeInfo("table1");

  private final int slotsPerBucket = 4;
  private final int bitsPerTag = 8;
  private final int bitsPerClock;
  private final int bitsPerSize;
  private final int bitsPerScope;
  private final ConcurrentClockCuckooFilter<PageId> filter;

  /**
   * @param cacheManager the real cache manager
   * @param conf the alluxio configuration
   */
  public CacheManagerWithCuckooShadowCache(CacheManager cacheManager, AlluxioConfiguration conf) {
    mCacheManager = cacheManager;

    long windowMs = conf.getMs(PropertyKey.USER_CLIENT_CACHE_SHADOW_WINDOW);
    long budgetInBits = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SHADOW_MEMORY_OVERHEAD) * 8;
    bitsPerClock = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_CLOCK_BITS);
    bitsPerSize = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_SIZE_BITS);
    bitsPerScope = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_SCOPE_BITS);
    long bitsPerSlot = bitsPerTag + bitsPerClock + bitsPerSize + bitsPerScope;
    long totalBuckets = budgetInBits / bitsPerSlot / slotsPerBucket;
    long expectedInsertions = Long.highestOneBit(totalBuckets);
    filter = ConcurrentClockCuckooFilter.create(
            CacheManagerWithShadowCache.PageIdFunnel.FUNNEL, expectedInsertions,
            bitsPerClock, bitsPerSize, bitsPerScope,
            SlidingWindowType.TIME_BASED, windowMs);

    long agingPeriod = windowMs >> bitsPerClock;
    mScheduler.scheduleAtFixedRate(filter::checkAging, agingPeriod, agingPeriod,
        MILLISECONDS);
  }

  /**
   * Stop to switch bloom filters and update working set size.
   */
  @VisibleForTesting
  public void stopUpdate() {
    mScheduler.shutdown();
  }

  /**
   * Update working set size in number of pages and bytes.
   */
  @VisibleForTesting
  public void updateWorkingSetSize() {
    long oldPages = Metrics.SHADOW_CACHE_PAGES.getCount();
    mShadowCachePages = filter.getItemNumber();
    Metrics.SHADOW_CACHE_PAGES.inc(mShadowCachePages - oldPages);
    long oldBytes = Metrics.SHADOW_CACHE_BYTES.getCount();
    mShadowCacheBytes = filter.getItemSize();
    Metrics.SHADOW_CACHE_BYTES.inc(mShadowCacheBytes - oldBytes);
  }

  @Override
  public boolean put(PageId pageId, byte[] page, CacheContext cacheContext) {
    updateClockCuckoo(pageId, page.length, cacheContext);
    return mCacheManager.put(pageId, page, cacheContext);
  }

  private void updateClockCuckoo(PageId pageId, int pageLength,
                                 CacheContext cacheContext) {
    if (!filter.mightContainAndResetClock(pageId)) {
      // TODO(iluoeli): get real scope
      filter.put(pageId, pageLength, scope);
      updateFalsePositiveRatio();
      updateWorkingSetSize();
      if (cacheContext != null) {
        cacheContext
            .incrementCounter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES.getName(), pageLength);
      }
    }
  }

  /**
   * Update the false positive ratio statistics.
   */
  private void updateFalsePositiveRatio() {
    int falsePositiveRatio = (int) filter.expectedFpp() * 100;
    long oldFalsePositiveRatio = Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.getCount();
    Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.inc(falsePositiveRatio - oldFalsePositiveRatio);
  }

  /**
   * Decrease each item's clock and clean stale items.
   */
  public void aging() {
    filter.checkAging();
  }

  /**
   * @return ShadowCachePages
   */
  public long getShadowCachePages() {
    return mShadowCachePages;
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCacheBytes() {
    return mShadowCacheBytes;
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCachePageRead() {
    return mShadowCachePageRead.get();
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCachePageHit() {
    return mShadowCachePageHit.get();
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCacheByteRead() {
    return mShadowCacheByteRead.get();
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCacheByteHit() {
    return mShadowCacheByteHit.get();
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
      int offsetInBuffer, CacheContext cacheContext) {
    boolean seen = filter.mightContainAndResetClock(pageId);
    if (seen) {
      Metrics.SHADOW_CACHE_PAGES_HIT.inc();
      Metrics.SHADOW_CACHE_BYTES_HIT.inc(bytesToRead);
      mShadowCachePageHit.getAndIncrement();
      mShadowCacheByteHit.getAndAdd(bytesToRead);
    } else {
      updateClockCuckoo(pageId, bytesToRead, cacheContext);
    }
    Metrics.SHADOW_CACHE_PAGES_READ.inc();
    Metrics.SHADOW_CACHE_BYTES_READ.inc(bytesToRead);
    mShadowCachePageRead.getAndIncrement();
    mShadowCacheByteRead.getAndAdd(bytesToRead);
    return mCacheManager.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, cacheContext);
  }

  @Override
  public boolean delete(PageId pageId) {
    return mCacheManager.delete(pageId);
  }

  @Override
  public State state() {
    return mCacheManager.state();
  }

  @Override
  public void close() throws Exception {
    mCacheManager.close();
  }

  /**
   * Funnel for PageId.
   */
  public enum PageIdFunnel implements Funnel<PageId> {
    FUNNEL;

    /**
     * @param from source
     * @param into destination
     */
    public void funnel(PageId from, PrimitiveSink into) {
      into.putUnencodedChars(from.getFileId()).putLong(from.getPageIndex());
    }
  }

  private static final class Metrics {
    private static final Counter SHADOW_CACHE_BYTES_READ =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES_READ.getName());
    private static final Counter SHADOW_CACHE_BYTES_HIT =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES_HIT.getName());
    private static final Counter SHADOW_CACHE_PAGES_READ =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_PAGES_READ.getName());
    private static final Counter SHADOW_CACHE_PAGES_HIT =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_PAGES_HIT.getName());
    private static final Counter SHADOW_CACHE_PAGES =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_PAGES.getName());
    private static final Counter SHADOW_CACHE_BYTES =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES.getName());
    private static final Counter SHADOW_CACHE_FALSE_POSITIVE_RATIO =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_FALSE_POSITIVE_RATIO.getName());
  }
}
