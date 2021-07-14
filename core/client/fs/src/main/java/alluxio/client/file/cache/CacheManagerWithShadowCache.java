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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
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
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A wrapper class of CacheManager with shadow cache.
 */
public class CacheManagerWithShadowCache implements CacheManager {
  private final CacheManager mCacheManager;
  private final int mNumBloomFilter;
  private final long mBloomFilterExpectedInsertions;
  // An array of bloom filters, and each capture a segment of window
  private final AtomicReferenceArray<BloomFilter<PageId>> mSegmentBloomFilters;
  private final AtomicIntegerArray mObjEachBloomFilter;
  private final AtomicLongArray mByteEachBloomFilter;
  private final AtomicLong mShadowCachePageRead = new AtomicLong(0);
  private final AtomicLong mShadowCachePageHit = new AtomicLong(0);
  private final ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
  private final AtomicLong mShadowCacheByteRead = new AtomicLong(0);
  private long mShadowCacheBytes = 0;
  private final AtomicLong mShadowCacheByteHit = new AtomicLong(0);
  private int mCurrentSegmentFilterIndex = 0;
  // capture the entire working set
  private BloomFilter<PageId> mWorkingSetBloomFilter;
  private long mShadowCachePages = 0;
  private double mAvgPageSize;

  /**
   * @param cacheManager the real cache manager
   * @param conf the alluxio configuration
   */
  public CacheManagerWithShadowCache(CacheManager cacheManager, AlluxioConfiguration conf) {
    mCacheManager = cacheManager;

    long windowMs = conf.getMs(PropertyKey.USER_CLIENT_CACHE_SHADOW_WINDOW);
    mNumBloomFilter = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_BLOOMFILTER_NUM);
    // include the 1 extra working set bloom filter
    long perBloomFilterMemoryOverhead =
        conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SHADOW_MEMORY_OVERHEAD) / (mNumBloomFilter + 1);
    // assume 3% Guava default false positive ratio
    mBloomFilterExpectedInsertions =
        (long) ((-perBloomFilterMemoryOverhead * Math.log(2) * Math.log(2)) / Math.log(0.03));
    mObjEachBloomFilter = new AtomicIntegerArray(new int[mNumBloomFilter]);
    mByteEachBloomFilter = new AtomicLongArray(new long[mNumBloomFilter]);
    mSegmentBloomFilters =
        new AtomicReferenceArray<BloomFilter<PageId>>(new BloomFilter[mNumBloomFilter]);
    for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
      mSegmentBloomFilters.set(i,
          BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions));
    }
    mWorkingSetBloomFilter =
        BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions);
    mScheduler.scheduleAtFixedRate(this::switchBloomFilter, 0, windowMs / mNumBloomFilter,
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
    updateAvgPageSize();
    long oldPages = Metrics.SHADOW_CACHE_PAGES.getCount();
    mShadowCachePages = (int) mWorkingSetBloomFilter.approximateElementCount();
    Metrics.SHADOW_CACHE_PAGES.inc(mShadowCachePages - oldPages);
    long oldBytes = Metrics.SHADOW_CACHE_BYTES.getCount();
    mShadowCacheBytes = (long) (mShadowCachePages * mAvgPageSize);
    Metrics.SHADOW_CACHE_BYTES.inc(mShadowCacheBytes - oldBytes);
  }

  @Override
  public boolean put(PageId pageId, byte[] page, CacheScope cacheScope, CacheQuota cacheQuota) {
    updateBloomFilterAndWorkingSet(pageId, page.length);
    return mCacheManager.put(pageId, page, cacheScope, cacheQuota);
  }

  private void updateBloomFilterAndWorkingSet(PageId pageId, int pageLength) {
    int filterIndex = mCurrentSegmentFilterIndex;
    BloomFilter<PageId> bf = mSegmentBloomFilters.get(filterIndex);
    if (!bf.mightContain(pageId)) {
      bf.put(pageId);
      mObjEachBloomFilter.getAndIncrement(filterIndex);
      mByteEachBloomFilter.getAndAdd(filterIndex, pageLength);
      mWorkingSetBloomFilter.put(pageId);
      updateFalsePositiveRatio();
      updateWorkingSetSize();
    }
  }

  /**
   * Update the false positive ratio statistics.
   */
  private void updateFalsePositiveRatio() {
    int falsePositiveRatio = (int) mWorkingSetBloomFilter.expectedFpp() * 100;
    long oldFalsePositiveRatio = Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.getCount();
    Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.inc(falsePositiveRatio - oldFalsePositiveRatio);
  }

  /**
   * Update the avg page size statistics.
   */
  private void updateAvgPageSize() {
    int nInsert = 0;
    long nByte = 0;
    for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
      nInsert += mObjEachBloomFilter.get(i);
      nByte += mByteEachBloomFilter.get(i);
    }
    if (nInsert == 0) {
      mAvgPageSize = 0;
    } else {
      mAvgPageSize = nByte / (double) nInsert;
    }
  }

  /**
   * Replace the oldest bloom filter with a new one.
   */
  public void switchBloomFilter() {
    // put here because if when put it in other function, there is a risk that mObj and mGet are
    // read inconsistently
    updateAvgPageSize();
    mCurrentSegmentFilterIndex = (mCurrentSegmentFilterIndex + 1) % mNumBloomFilter;
    mSegmentBloomFilters.set(mCurrentSegmentFilterIndex,
        BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions));
    mObjEachBloomFilter.set(mCurrentSegmentFilterIndex, 0);
    mByteEachBloomFilter.set(mCurrentSegmentFilterIndex, 0);
    mWorkingSetBloomFilter =
        BloomFilter.create(PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions);
    for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
      mWorkingSetBloomFilter.putAll(mSegmentBloomFilters.get(i));
    }
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
      int offsetInBuffer) {
    boolean seen = false;
    for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
      seen |= mSegmentBloomFilters.get(i).mightContain(pageId);
    }
    if (seen) {
      Metrics.SHADOW_CACHE_PAGES_HIT.inc();
      Metrics.SHADOW_CACHE_BYTES_HIT.inc(bytesToRead);
      mShadowCachePageHit.getAndIncrement();
      mShadowCacheByteHit.getAndAdd(bytesToRead);
    } else {
      updateBloomFilterAndWorkingSet(pageId, bytesToRead);
    }
    Metrics.SHADOW_CACHE_PAGES_READ.inc();
    Metrics.SHADOW_CACHE_BYTES_READ.inc(bytesToRead);
    mShadowCachePageRead.getAndIncrement();
    mShadowCacheByteRead.getAndAdd(bytesToRead);
    return mCacheManager.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer);
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
