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

import static java.util.concurrent.TimeUnit.SECONDS;

import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
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
  private static final int NUM_BLOOM_FILTER = 4;
  private static final int NUM_BLOOM_FILTER_EXPECT_INSERTION = 20_000_000;
  private static final double NUM_BLOOM_FILTER_FPP = .00819;
  private final CacheManager mCacheManager;
  private final AtomicReferenceArray<BloomFilter<PageId>> mBFs =
      new AtomicReferenceArray<BloomFilter<PageId>>(new BloomFilter[NUM_BLOOM_FILTER]);
  private final AtomicIntegerArray mObjInserted = new AtomicIntegerArray(new int[NUM_BLOOM_FILTER]);
  private final AtomicLongArray mByteInserted = new AtomicLongArray(new long[NUM_BLOOM_FILTER]);
  ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
  // working set size window, in unit of second
  private int mWindow = 20;
  private int mCurrentFilterIndex = 0;
  private int mShadowCachePages = 0;
  private long mShadowCacheBytes = 0;
  private AtomicLong mShadowCachePageRead = new AtomicLong(0);
  private AtomicLong mShadowCachePageHit = new AtomicLong(0);
  private AtomicLong mShadowCacheByteRead = new AtomicLong(0);
  private AtomicLong mShadowCacheByteHit = new AtomicLong(0);

  /**
   * @param cacheManager the real cache manager
   * @param window the past window in which is the working set
   */
  public CacheManagerWithShadowCache(CacheManager cacheManager, int window) {
    mWindow = window;
    mCacheManager = cacheManager;
    for (int i = 0; i < mBFs.length(); ++i) {
      mBFs.set(i, BloomFilter.create(PageIdFunnel.FUNNEL, NUM_BLOOM_FILTER_EXPECT_INSERTION,
          NUM_BLOOM_FILTER_FPP));
    }
    mScheduler.scheduleAtFixedRate(this::switchBloomFilter, 0, mWindow / 4, SECONDS);
    // update working set size every sec
    mScheduler.scheduleAtFixedRate(this::updateWorkingSetSize, 0, 1, SECONDS);
  }

  /**
   * Stop to switch bloom filters and update working set size.
   */
  public void stopUpdate() {
    mScheduler.shutdown();
  }

  @Override
  public boolean put(PageId pageId, byte[] page, CacheScope cacheScope, CacheQuota cacheQuota) {
    if (!mBFs.get(mCurrentFilterIndex).mightContain(pageId)) {
      mBFs.get(mCurrentFilterIndex).put(pageId);
      mObjInserted.getAndIncrement(mCurrentFilterIndex);
      mByteInserted.getAndAdd(mCurrentFilterIndex, page.length);
    }
    return mCacheManager.put(pageId, page, cacheScope, cacheQuota);
  }

  /**
   * Replace the oldest bloom filter with a new one.
   */
  public void switchBloomFilter() {
    mCurrentFilterIndex = (mCurrentFilterIndex + 1) % NUM_BLOOM_FILTER;
    mBFs.set(mCurrentFilterIndex, BloomFilter.create(PageIdFunnel.FUNNEL,
        NUM_BLOOM_FILTER_EXPECT_INSERTION, NUM_BLOOM_FILTER_FPP));
    mObjInserted.set(mCurrentFilterIndex, 0);
    mByteInserted.set(mCurrentFilterIndex, 0);
  }

  /**
   * Update working set size in number of pages and bytes.
   */
  public void updateWorkingSetSize() {
    BloomFilter<PageId> bf =
        BloomFilter.create(PageIdFunnel.FUNNEL, NUM_BLOOM_FILTER_EXPECT_INSERTION,
            NUM_BLOOM_FILTER_FPP);
    int nInsert = 0;
    long nByte = 0;
    for (int i = 0; i < mBFs.length(); ++i) {
      bf.putAll(mBFs.get(i));
      nInsert += mObjInserted.get(i);
      nByte += mByteInserted.get(i);
    }
    double avgPageSize;
    if (nInsert == 0) {
      avgPageSize = 0;
    } else {
      avgPageSize = nByte / (double) nInsert;
    }
    long oldPages = Metrics.SHADOW_CACHE_PAGES.getCount();
    mShadowCachePages = (int) bf.approximateElementCount();
    Metrics.SHADOW_CACHE_PAGES.inc(mShadowCachePages - oldPages);
    long oldBytes = Metrics.SHADOW_CACHE_BYTES.getCount();
    mShadowCacheBytes = (long) (mShadowCachePages * avgPageSize);
    Metrics.SHADOW_CACHE_BYTES.inc(mShadowCacheBytes - oldBytes);
  }

  /**
   * @return ShadowCachePages
   */
  public int getShadowCachePages() {
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
    for (int i = 0; i < mBFs.length(); ++i) {
      seen |= mBFs.get(i).mightContain(pageId);
    }
    if (seen) {
      Metrics.SHADOW_CACHE_PAGES_HIT.inc();
      Metrics.SHADOW_CACHE_BYTES_HIT.inc(bytesToRead);
      mShadowCachePageHit.getAndIncrement();
      mShadowCacheByteHit.getAndAdd(bytesToRead);
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
  }
}
