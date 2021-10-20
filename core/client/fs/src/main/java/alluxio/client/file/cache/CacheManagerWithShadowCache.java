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
import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A wrapper class of CacheManager with shadow cache.
 */
public class CacheManagerWithShadowCache implements CacheManager {
  private final CacheManager mCacheManager;
  private final ShadowCacheManager mShadowCacheManager;
  private long mShadowCachePages = 0;
  private long mShadowCacheBytes = 0;
  private final AtomicLong mShadowCachePageRead = new AtomicLong(0);
  private final AtomicLong mShadowCachePageHit = new AtomicLong(0);
  private final AtomicLong mShadowCacheByteRead = new AtomicLong(0);
  private final AtomicLong mShadowCacheByteHit = new AtomicLong(0);

  /**
   * @param cacheManager the real cache manager
   * @param conf the alluxio configuration
   */
  public CacheManagerWithShadowCache(CacheManager cacheManager, AlluxioConfiguration conf) {
    mCacheManager = cacheManager;
    mShadowCacheManager = ShadowCacheManager.create(conf);
  }

  @Override
  public boolean put(PageId pageId, byte[] page, CacheContext cacheContext) {
    updateShadowCache(pageId, page.length, cacheContext);
    return mCacheManager.put(pageId, page, cacheContext);
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer,
      CacheContext cacheContext) {
    boolean seen = updateShadowCache(pageId, bytesToRead, cacheContext);
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
    return mCacheManager.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, cacheContext);
  }

  /**
   * @param pageId page identifier
   * @param pageLength length of this page
   * @param cacheContext cache related context
   * @return true if it is an new page; false otherwise
   */
  private boolean updateShadowCache(PageId pageId, int pageLength, CacheContext cacheContext) {
    CacheScope cacheScope =
        (cacheContext == null) ? CacheScope.GLOBAL : cacheContext.getCacheScope();
    boolean seen = mShadowCacheManager.read(pageId, pageLength, cacheScope);
    if (!seen) {
      mShadowCacheManager.put(pageId, pageLength, cacheScope);
      updateFalsePositiveRatio();
      updateWorkingSetSize();
      if (cacheContext != null) {
        cacheContext.incrementCounter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES.getName(),
            pageLength);
      }
    }
    return seen;
  }

  /**
   * Update the false positive ratio statistics.
   */
  private void updateFalsePositiveRatio() {
    int falsePositiveRatio = (int) mShadowCacheManager.getFalsePositiveRatio() * 100;
    long oldFalsePositiveRatio = Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.getCount();
    Metrics.SHADOW_CACHE_FALSE_POSITIVE_RATIO.inc(falsePositiveRatio - oldFalsePositiveRatio);
  }

  /**
   * Update working set size in number of pages and bytes.
   */
  @VisibleForTesting
  public void updateWorkingSetSize() {
    mShadowCacheManager.updateWorkingSetSize();
    long oldPages = Metrics.SHADOW_CACHE_PAGES.getCount();
    mShadowCachePages = mShadowCacheManager.getShadowCachePages();
    Metrics.SHADOW_CACHE_PAGES.inc(mShadowCachePages - oldPages);
    long oldBytes = Metrics.SHADOW_CACHE_BYTES.getCount();
    mShadowCacheBytes = mShadowCacheManager.getShadowCacheBytes();
    Metrics.SHADOW_CACHE_BYTES.inc(mShadowCacheBytes - oldBytes);
  }

  @Override
  public boolean delete(PageId pageId) {
    return mCacheManager.delete(pageId);
  }

  /**
   * Stop to switch bloom filters and update working set size.
   */
  @VisibleForTesting
  public void stopUpdate() {
    mShadowCacheManager.stopUpdate();
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
   * Decrease each item's clock and clean stale items.
   */
  public void aging() {
    mShadowCacheManager.aging();
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
