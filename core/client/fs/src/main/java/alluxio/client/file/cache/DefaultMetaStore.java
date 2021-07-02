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

import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
  /** The evictor. */
  private final CacheEvictor mEvictor;

  /**
   * @param conf configuration
   */
  public DefaultMetaStore(AlluxioConfiguration conf) {
    this(CacheEvictor.create(conf));
  }

  /**
   * @param evictor cache evictor
   */
  @VisibleForTesting
  public DefaultMetaStore(CacheEvictor evictor) {
    mEvictor = evictor;
  }

  @Override
  public boolean hasPage(PageId pageId) {
    return mPageMap.containsKey(pageId);
  }

  @Override
  public void addPage(PageId pageId, PageInfo pageInfo) {
    mPageMap.put(pageId, pageInfo);
    mBytes.addAndGet(pageInfo.getPageSize());
    Metrics.SPACE_USED.inc(pageInfo.getPageSize());
    mPages.incrementAndGet();
    Metrics.PAGES.inc();
    mEvictor.updateOnPut(pageId);
  }

  @Override
  public PageInfo getPageInfo(PageId pageId) throws PageNotFoundException {
    if (!mPageMap.containsKey(pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
    mEvictor.updateOnGet(pageId);
    return mPageMap.get(pageId);
  }

  @Override
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    if (!mPageMap.containsKey(pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
    PageInfo pageInfo = mPageMap.remove(pageId);
    mBytes.addAndGet(-pageInfo.getPageSize());
    Metrics.SPACE_USED.dec(pageInfo.getPageSize());
    mPages.decrementAndGet();
    Metrics.PAGES.dec();
    mEvictor.updateOnDelete(pageId);
    return pageInfo;
  }

  @Override
  public long bytes() {
    return mBytes.get();
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
    mEvictor.reset();
  }

  @Override
  @Nullable
  public PageInfo evict() {
    return evictInternal(mEvictor);
  }

  PageInfo evictInternal(CacheEvictor evictor) {
    PageId victim = evictor.evict();
    if (victim == null) {
      return null;
    }
    PageInfo victimInfo = mPageMap.get(victim);
    if (victimInfo == null) {
      LOG.error("Invalid result returned by evictor: page {} not available", victim);
      evictor.updateOnDelete(victim);
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
