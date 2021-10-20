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

import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.hash.BloomFilter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This class is a shadow cache with multiple bloom filter implementation.
 */
public class MultipleBloomShadowCacheManager implements ShadowCacheManager {
  private final int mNumBloomFilter;
  private final long mBloomFilterExpectedInsertions;
  // An array of bloom filters, and each capture a segment of window
  private final AtomicReferenceArray<BloomFilter<PageId>> mSegmentBloomFilters;
  private final AtomicIntegerArray mObjEachBloomFilter;
  private final AtomicLongArray mByteEachBloomFilter;
  private final ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
  private int mCurrentSegmentFilterIndex = 0;
  // capture the entire working set
  private BloomFilter<PageId> mWorkingSetBloomFilter;
  private long mShadowCachePages = 0;
  private long mShadowCacheBytes = 0;
  private double mAvgPageSize;

  /**
   * Create a MultipleBloomShadowCacheManager.
   *
   * @param conf the alluxio configuration
   */
  public MultipleBloomShadowCacheManager(AlluxioConfiguration conf) {
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
      mSegmentBloomFilters.set(i, BloomFilter
          .create(CacheManagerWithShadowCache.PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions));
    }
    mWorkingSetBloomFilter = BloomFilter.create(CacheManagerWithShadowCache.PageIdFunnel.FUNNEL,
        mBloomFilterExpectedInsertions);
    mScheduler.scheduleAtFixedRate(this::switchBloomFilter, 0, windowMs / mNumBloomFilter,
        MILLISECONDS);
  }

  @Override
  public boolean put(PageId pageId, int size, CacheScope scope) {
    updateBloomFilterAndWorkingSet(pageId, size);
    return true;
  }

  @Override
  public boolean read(PageId pageId, int size, CacheScope scope) {
    boolean seen = false;
    for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
      seen |= mSegmentBloomFilters.get(i).mightContain(pageId);
    }
    return seen;
  }

  /**
   * Put a page into current bloom filter and working set bloom filter.
   *
   * @param pageId page identifier
   * @param pageLength page size
   */
  private void updateBloomFilterAndWorkingSet(PageId pageId, int pageLength) {
    int filterIndex = mCurrentSegmentFilterIndex;
    BloomFilter<PageId> bf = mSegmentBloomFilters.get(filterIndex);
    if (!bf.mightContain(pageId)) {
      bf.put(pageId);
      mObjEachBloomFilter.getAndIncrement(filterIndex);
      mByteEachBloomFilter.getAndAdd(filterIndex, pageLength);
      mWorkingSetBloomFilter.put(pageId);
      updateWorkingSetSize();
    }
  }

  @Override
  public void updateWorkingSetSize() {
    updateAvgPageSize();
    mShadowCachePages = (int) mWorkingSetBloomFilter.approximateElementCount();
    mShadowCacheBytes = (long) (mShadowCachePages * mAvgPageSize);
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

  @Override
  public boolean delete(PageId pageId) {
    // since bloom filter does not support deleting, always return false here
    return false;
  }

  @Override
  public void aging() {
    switchBloomFilter();
  }

  /**
   * Replace the oldest bloom filter with a new one.
   */
  private void switchBloomFilter() {
    // put here because if when put it in other function, there is a risk that mObj and mGet are
    // read inconsistently
    updateAvgPageSize();
    mCurrentSegmentFilterIndex = (mCurrentSegmentFilterIndex + 1) % mNumBloomFilter;
    mSegmentBloomFilters.set(mCurrentSegmentFilterIndex, BloomFilter
        .create(CacheManagerWithShadowCache.PageIdFunnel.FUNNEL, mBloomFilterExpectedInsertions));
    mObjEachBloomFilter.set(mCurrentSegmentFilterIndex, 0);
    mByteEachBloomFilter.set(mCurrentSegmentFilterIndex, 0);
    mWorkingSetBloomFilter = BloomFilter.create(CacheManagerWithShadowCache.PageIdFunnel.FUNNEL,
        mBloomFilterExpectedInsertions);
    for (int i = 0; i < mSegmentBloomFilters.length(); ++i) {
      mWorkingSetBloomFilter.putAll(mSegmentBloomFilters.get(i));
    }
  }

  @Override
  public void stopUpdate() {
    mScheduler.shutdown();
  }

  @Override
  public long getShadowCachePages() {
    return mShadowCachePages;
  }

  @Override
  public long getShadowCachePages(CacheScope scope) {
    return 0;
  }

  @Override
  public long getShadowCacheBytes() {
    return mShadowCacheBytes;
  }

  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    return 0;
  }

  @Override
  public double getFalsePositiveRatio() {
    return mWorkingSetBloomFilter.expectedFpp();
  }
}
