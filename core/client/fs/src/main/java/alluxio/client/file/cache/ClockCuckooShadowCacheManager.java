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

import static alluxio.client.file.cache.cuckoofilter.ConcurrentClockCuckooFilter.DEFAULT_FPP;
import static alluxio.client.file.cache.cuckoofilter.ConcurrentClockCuckooFilter.DEFAULT_LOAD_FACTOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import alluxio.client.file.cache.cuckoofilter.ClockCuckooFilter;
import alluxio.client.file.cache.cuckoofilter.ConcurrentClockCuckooFilter;
import alluxio.client.file.cache.cuckoofilter.SlidingWindowType;
import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.hash.Hashing;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is a shadow cache with {@link ClockCuckooFilter} implementation.
 */
public class ClockCuckooShadowCacheManager implements ShadowCacheManager {
  private static final int BITS_PER_TAG = 8;

  private final ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
  private final ClockCuckooFilter<PageId> mFilter;
  private long mShadowCacheBytes = 0;
  private long mShadowCachePages = 0;

  private final AtomicLong mShadowCachePageRead = new AtomicLong(0);
  private final AtomicLong mShadowCachePageHit = new AtomicLong(0);
  private final AtomicLong mShadowCacheByteRead = new AtomicLong(0);
  private final AtomicLong mShadowCacheByteHit = new AtomicLong(0);

  /**
   * Create a ClockCuckooShadowCacheManager.
   *
   * @param conf the alluxio configuration
   */
  public ClockCuckooShadowCacheManager(AlluxioConfiguration conf) {
    long windowMs = conf.getMs(PropertyKey.USER_CLIENT_CACHE_SHADOW_WINDOW);
    long budgetInBits = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SHADOW_MEMORY_OVERHEAD) * 8;
    int bitsPerClock = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_CLOCK_BITS);
    int bitsPerSize = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_SIZE_BITS);
    int bitsPerScope = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_SCOPE_BITS);
    boolean sizeEncoder = conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_SIZE_ENCODER);
    long bitsPerSlot = BITS_PER_TAG + bitsPerClock + bitsPerSize + bitsPerScope;
    long totalSlots = budgetInBits / bitsPerSlot;
    long expectedInsertions = (long) (Long.highestOneBit(totalSlots) * DEFAULT_LOAD_FACTOR);
    if (sizeEncoder) {
      int prefixBits = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_SIZE_PREFIX_BITS);
      int suffixBits = conf.getInt(PropertyKey.USER_CLIENT_CACHE_SHADOW_CUCKOO_SIZE_SUFFIX_BITS);
      mFilter = ConcurrentClockCuckooFilter.create(CacheManagerWithShadowCache.PageIdFunnel.FUNNEL,
          expectedInsertions, bitsPerClock, bitsPerScope, prefixBits, suffixBits,
          SlidingWindowType.TIME_BASED, windowMs, DEFAULT_FPP, DEFAULT_LOAD_FACTOR,
          Hashing.murmur3_128());
    } else {
      mFilter = ConcurrentClockCuckooFilter.create(CacheManagerWithShadowCache.PageIdFunnel.FUNNEL,
          expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope, SlidingWindowType.TIME_BASED,
          windowMs);
    }
    long agingPeriod = windowMs >> bitsPerClock;
    mScheduler.scheduleAtFixedRate(this::aging, agingPeriod, agingPeriod, MILLISECONDS);
  }

  @Override
  public boolean put(PageId pageId, int size, CacheScope cacheScope) {
    return updateClockCuckoo(pageId, size, cacheScope);
  }

  /**
   * Put a page into shadow cache if it is not existed.
   *
   * @param pageId     page identifier
   * @param size       page size
   * @param cacheScope cache scope
   * @return true if page is put successfully; false otherwise
   */
  private boolean updateClockCuckoo(PageId pageId, int size, CacheScope cacheScope) {
    boolean ok = true;
    if (!mFilter.mightContainAndResetClock(pageId)) {
      ok = mFilter.put(pageId, size, cacheScope);
      updateWorkingSetSize();
    }
    return ok;
  }

  @Override
  public int get(PageId pageId, int bytesToRead, CacheScope cacheScope) {
    mShadowCachePageRead.getAndIncrement();
    mShadowCacheByteRead.getAndAdd(bytesToRead);
    if (mFilter.mightContainAndResetClock(pageId)) {
      mShadowCachePageHit.getAndIncrement();
      mShadowCacheByteHit.getAndAdd(bytesToRead);
      return bytesToRead;
    }
    return 0;
  }

  @Override
  public boolean delete(PageId pageId) {
    return mFilter.delete(pageId);
  }

  @Override
  public void aging() {
    mFilter.aging();
  }

  @Override
  public void updateWorkingSetSize() {
    mShadowCachePages = mFilter.approximateElementCount();
    mShadowCacheBytes = mFilter.approximateElementSize();
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
    return mFilter.approximateElementCount(scope);
  }

  @Override
  public long getShadowCacheBytes() {
    return mShadowCacheBytes;
  }

  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    return mFilter.approximateElementSize(scope);
  }

  @Override
  public long getShadowCachePageRead() {
    return mShadowCachePageRead.get();
  }

  @Override
  public long getShadowCachePageHit() {
    return mShadowCachePageHit.get();
  }

  @Override
  public long getShadowCacheByteRead() {
    return mShadowCacheByteRead.get();
  }

  @Override
  public long getShadowCacheByteHit() {
    return mShadowCacheByteHit.get();
  }

  @Override
  public double getFalsePositiveRatio() {
    return mFilter.expectedFpp();
  }
}
