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

import static alluxio.client.file.CacheContext.StatsUnit.BYTE;

import alluxio.client.file.CacheContext;
import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import javax.annotation.Nonnull;

/**
 * A wrapper class of CacheManager with shadow cache.
 */
public class CacheManagerWithShadowCache implements CacheManager {
  private final CacheManager mCacheManager;
  private final ShadowCacheManager mShadowCacheManager;
  private long mShadowCachePages = 0;
  private long mShadowCacheBytes = 0;

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
    int nread = mShadowCacheManager.get(pageId, bytesToRead, getCacheScope(cacheContext));
    if (nread > 0) {
      Metrics.SHADOW_CACHE_PAGES_HIT.inc();
      Metrics.SHADOW_CACHE_BYTES_HIT.inc(nread);
    } else {
      updateShadowCache(pageId, bytesToRead, cacheContext);
    }
    Metrics.SHADOW_CACHE_PAGES_READ.inc();
    Metrics.SHADOW_CACHE_BYTES_READ.inc(bytesToRead);
    return mCacheManager.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, cacheContext);
  }

  /**
   * @param pageId page identifier
   * @param pageLength length of this page
   * @param cacheContext cache related context
   */
  private void updateShadowCache(PageId pageId, int pageLength, CacheContext cacheContext) {
    if (mShadowCacheManager.put(pageId, pageLength, getCacheScope(cacheContext))) {
      updateFalsePositiveRatio();
      updateWorkingSetSize();
      if (cacheContext != null) {
        cacheContext.incrementCounter(MetricKey.CLIENT_CACHE_SHADOW_CACHE_BYTES.getName(), BYTE,
            pageLength);
      }
    }
  }

  /**
   * @param cacheContext cache related context
   * @return the cache scope
   */
  private CacheScope getCacheScope(CacheContext cacheContext) {
    return (cacheContext == null) ? CacheScope.GLOBAL : cacheContext.getCacheScope();
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
  public boolean append(PageId pageId, int appendAt, byte[] page, CacheContext cacheContext) {
    return mCacheManager.append(pageId, appendAt, page, cacheContext);
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
    return mShadowCacheManager.getShadowCachePageRead();
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCachePageHit() {
    return mShadowCacheManager.getShadowCachePageHit();
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCacheByteRead() {
    return mShadowCacheManager.getShadowCacheByteRead();
  }

  /**
   * @return ShadowCacheBytes
   */
  public long getShadowCacheByteHit() {
    return mShadowCacheManager.getShadowCacheByteHit();
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
    @Override
    public void funnel(@Nonnull PageId from, PrimitiveSink into) {
      into.putUnencodedChars(from.getFileId()).putLong(from.getPageIndex());
    }
  }

  private static final class Metrics {
    // Note that only counter can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
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
