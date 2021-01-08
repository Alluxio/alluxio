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

import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import javax.annotation.Nullable;

/**
 * The default implementation of a metadata store for pages stored in cache. This implementation
 * is not thread safe and requires synchronizations on external callers.
 */
public class DefaultMetaStore implements MetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetaStore.class);
  /** A map from PageId to page info. */
  private final Map<PageId, PageInfo> mPageMap = new HashMap<>();
  /** The number of logical bytes used. */
  private final AtomicLong mBytes = new AtomicLong(0);
  /** The number of pages stored. */
  private final AtomicLong mPages = new AtomicLong(0);
  /** Track the number of bytes on each scope. */
  private final Map<CacheScope, Long> mBytesInScope = new ConcurrentHashMap<>();
  private final boolean mQuotaEnabled;
  /** The evictor. */
  private final CacheEvictor mEvictor;
  private Map<CacheScope, CacheEvictor> mCacheEvictors;
  private Supplier<CacheEvictor> mSupplier;

  /**
   * @param conf configuration
   */
  public DefaultMetaStore(AlluxioConfiguration conf) {
    mQuotaEnabled = conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED);
    if (mQuotaEnabled) {
      mCacheEvictors = new ConcurrentHashMap<>();
      mSupplier = () -> CacheEvictor.create(conf);
      mEvictor = null;
    } else {
      mCacheEvictors = null;
      mSupplier = null;
      mEvictor = CacheEvictor.create(conf);
    }
  }

  /**
   * @param conf configuration
   * @param evictor cache evictor
   */
  @VisibleForTesting
  public DefaultMetaStore(AlluxioConfiguration conf, CacheEvictor evictor) {
    mQuotaEnabled = conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED);
    mEvictor = evictor;
  }

  @Override
  public boolean hasPage(PageId pageId) {
    return mPageMap.containsKey(pageId);
  }

  @Override
  public void addPage(PageId pageId, PageInfo pageInfo) {
    mPageMap.put(pageId, pageInfo);
    mPages.incrementAndGet();
    Metrics.SPACE_USED.inc(pageInfo.getPageSize());
    Metrics.PAGES.inc();
    if (mQuotaEnabled) {
      for (CacheScope cacheScope = pageInfo.getScope(); cacheScope != null; cacheScope = cacheScope.parent()) {
        mBytesInScope.compute(cacheScope,
            (k, v) -> (v == null) ? pageInfo.getPageSize() : v + pageInfo.getPageSize());
        CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
        evictor.updateOnPut(pageId);
      }
    } else {
      mBytes.addAndGet(pageInfo.getPageSize());
      mEvictor.updateOnPut(pageId);
    }
  }

  @Override
  public PageInfo getPageInfo(PageId pageId) throws PageNotFoundException {
    if (!mPageMap.containsKey(pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
    PageInfo pageInfo = mPageMap.get(pageId);
    if (mQuotaEnabled) {
      for (CacheScope cacheScope = pageInfo.getScope(); cacheScope != null; cacheScope = cacheScope.parent()) {
        CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
        evictor.updateOnPut(pageId);
      }
    } else {
      mEvictor.updateOnGet(pageId);
    }
    return pageInfo;
  }

  @Override
  public void removePage(PageId pageId) throws PageNotFoundException {
    if (!mPageMap.containsKey(pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
    PageInfo pageInfo = mPageMap.remove(pageId);
    mPages.decrementAndGet();
    Metrics.SPACE_USED.dec(pageInfo.getPageSize());
    Metrics.PAGES.dec();
    if (mQuotaEnabled) {
      for (CacheScope cacheScope = pageInfo.getScope(); cacheScope != null; cacheScope = cacheScope.parent()) {
        mBytesInScope.computeIfPresent(cacheScope, (k, v) -> v - pageInfo.getPageSize());
        CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
        evictor.updateOnDelete(pageId);
      }
    } else {
      mBytes.addAndGet(-pageInfo.getPageSize());
      mEvictor.updateOnDelete(pageId);
    }
  }

  @Override
  public long bytes() {
    return mBytes.get();
  }

  @Override
  public long bytes(CacheScope cacheScope) {
    if (mQuotaEnabled) {
      return mBytesInScope.computeIfAbsent(cacheScope, k -> 0L);
    } else {
      return bytes();
    }
  }

  @Override
  public long pages() {
    return mPages.get();
  }

  @Override
  public void reset() {
    mPages.set(0);
    Metrics.PAGES.dec(Metrics.PAGES.getCount());
    mBytes.set(0);
    Metrics.SPACE_USED.dec(Metrics.SPACE_USED.getCount());
    mPageMap.clear();
    if (mQuotaEnabled) {
      for (CacheEvictor evictor : mCacheEvictors.values()) {
        evictor.reset();
      }
    } else {
      mEvictor.reset();
    }
    mBytesInScope.clear();
  }

  @Override
  @Nullable
  public PageInfo evict(CacheScope cacheScope) {
    PageId victim;
    if (mQuotaEnabled) {
      CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
      victim = evictor.evict();
    } else {
      victim = mEvictor.evict();
    }
    if (victim == null) {
      return null;
    }
    PageInfo victimInfo = mPageMap.get(victim);
    if (victimInfo == null) {
      LOG.error("Invalid result returned by evictor: page {} not available", victim);
      mEvictor.updateOnDelete(victim);
      return null;
    }
    return victimInfo;
  }

  private static final class Metrics {
    /** Bytes used in the cache. */
    private static final Counter SPACE_USED =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SPACE_USED_COUNT.getName());
    /** Pages stored in the cache. */
    private static final Counter PAGES =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PAGES.getName());
  }
}
