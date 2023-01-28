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

import static java.util.Objects.requireNonNull;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.allocator.Allocator;
import alluxio.client.file.cache.allocator.HashAllocator;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.quota.CacheScope;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.PageNotFoundException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The default implementation of a metadata store for pages stored in cache. This implementation
 * is not thread safe and requires synchronizations on external callers by acquiring the associated
 * lock.
 */
@NotThreadSafe
public class DefaultPageMetaStore implements PageMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPageMetaStore.class);
  /** A map from PageId to page info. */
  private final IndexedSet<PageInfo> mPages = new IndexedSet<>(INDEX_PAGE_ID, INDEX_FILE_ID);
  private final ImmutableList<PageStoreDir> mDirs;
  /** The number of logical bytes used. */
  private final AtomicLong mBytes = new AtomicLong(0);

  protected final ReentrantReadWriteLock mLock = new ReentrantReadWriteLock();
  private final Allocator mAllcator;

  private static final IndexDefinition<PageInfo, PageId> INDEX_PAGE_ID =
      IndexDefinition.ofUnique(PageInfo::getPageId);
  private static final IndexDefinition<PageInfo, String> INDEX_FILE_ID =
      IndexDefinition.ofNonUnique(pageInfo -> pageInfo.getPageId().getFileId());

  /**
   * @param dirs storage directories
   */
  public DefaultPageMetaStore(List<PageStoreDir> dirs) {
    this(dirs, new HashAllocator(dirs));
  }

  /**
   * Constructor of DefaultMetaStore.
   *
   * @param dirs storage directories
   * @param allocator storage allocator
   */
  public DefaultPageMetaStore(List<PageStoreDir> dirs, Allocator allocator) {
    mDirs = ImmutableList.copyOf(requireNonNull(dirs));
    mAllcator = requireNonNull(allocator);
    //metrics for the num of pages stored in the cache
    MetricsSystem.registerGaugeIfAbsent(MetricKey.CLIENT_CACHE_PAGES.getName(),
        mPages::size);
  }

  @Override
  public ReentrantReadWriteLock getLock() {
    return mLock;
  }

  @Override
  @GuardedBy("getLock()")
  public boolean hasPage(PageId pageId) {
    return mPages.contains(INDEX_PAGE_ID, pageId);
  }

  @Override
  @GuardedBy("getLock()")
  public void addPage(PageId pageId, PageInfo pageInfo) {
    addPageInternal(pageId, pageInfo);
    pageInfo.getLocalCacheDir().putPage(pageInfo);
  }

  private void addPageInternal(PageId pageId, PageInfo pageInfo) {
    Preconditions.checkArgument(pageId.equals(pageInfo.getPageId()), "page id mismatch");
    mPages.add(pageInfo);
    mBytes.addAndGet(pageInfo.getPageSize());
    Metrics.SPACE_USED.inc(pageInfo.getPageSize());
  }

  @Override
  @GuardedBy("getLock()")
  public void addTempPage(PageId pageId, PageInfo pageInfo) {
    addPageInternal(pageId, pageInfo);
    pageInfo.getLocalCacheDir().putTempPage(pageInfo);
  }

  @Override
  @GuardedBy("getLock().writeLock()")
  public void commitFile(String fileId, String newFileId) throws PageNotFoundException {
    Set<PageInfo> pages = mPages.getByField(INDEX_FILE_ID, fileId);
    if (pages.size() == 0) {
      throw new PageNotFoundException(
          String.format("No Pages found for file %s when committing", fileId));
    }
    for (PageInfo oldPage : pages) {
      PageId newPageId = new PageId(newFileId, oldPage.getPageId().getPageIndex());
      PageInfo newPageInfo = new PageInfo(newPageId, oldPage.getPageSize(), oldPage.getScope(),
          oldPage.getLocalCacheDir());
      mPages.remove(oldPage);
      mPages.add(newPageInfo);
    }
  }

  @Override
  public List<PageStoreDir> getStoreDirs() {
    return mDirs;
  }

  @Override
  public PageStoreDir allocate(String fileId, long fileLength) {
    return mAllcator.allocate(fileId, fileLength);
  }

  @Override
  @GuardedBy("getLock()")
  public PageInfo getPageInfo(PageId pageId) throws PageNotFoundException {
    if (!mPages.contains(INDEX_PAGE_ID, pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
    PageInfo pageInfo = mPages.getFirstByField(INDEX_PAGE_ID, pageId);
    pageInfo.getLocalCacheDir().getEvictor().updateOnGet(pageId);
    return pageInfo;
  }

  @Override
  @GuardedBy("getLock()")
  public PageInfo removeTempPage(PageId pageId, CacheContext cacheContext)
      throws PageNotFoundException {
    if (!mPages.contains(INDEX_PAGE_ID, pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }

    PageInfo pageInfo = mPages.getFirstByField(INDEX_PAGE_ID, pageId);
    mPages.remove(pageInfo);
    mBytes.addAndGet(-pageInfo.getPageSize());
    Metrics.SPACE_USED.dec(pageInfo.getPageSize());
    pageInfo.getLocalCacheDir().deleteTempPage(pageInfo);
    return pageInfo;
  }

  @Override
  @GuardedBy("getLock()")
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    if (!mPages.contains(INDEX_PAGE_ID, pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }

    PageInfo pageInfo = mPages.getFirstByField(INDEX_PAGE_ID, pageId);
    mPages.remove(pageInfo);
    mBytes.addAndGet(-pageInfo.getPageSize());
    Metrics.SPACE_USED.dec(pageInfo.getPageSize());
    pageInfo.getLocalCacheDir().deletePage(pageInfo);
    return pageInfo;
  }

  @Override
  public long bytes() {
    return mBytes.get();
  }

  @Override
  @GuardedBy("getLock()")
  public long numPages() {
    return mPages.size();
  }

  @Override
  @GuardedBy("getLock()")
  public void reset() {
    mBytes.set(0);
    Metrics.SPACE_USED.dec(Metrics.SPACE_USED.getCount());
    mPages.clear();
  }

  @Override
  @Nullable
  @GuardedBy("getLock()")
  public PageInfo evict(CacheScope scope, PageStoreDir pageStoreDir) {
    return evictInternal(pageStoreDir.getEvictor());
  }

  PageInfo evictInternal(CacheEvictor evictor) {
    PageId victim = evictor.evict();
    if (victim == null) {
      return null;
    }
    PageInfo victimInfo = mPages.getFirstByField(INDEX_PAGE_ID, victim);
    if (victimInfo == null) {
      LOG.error("Invalid result returned by evictor: page {} not available", victim);
      evictor.updateOnDelete(victim);
      return null;
    }
    return victimInfo;
  }

  private static final class Metrics {
    // Note that only counter can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
    /** Bytes used in the cache. */
    private static final Counter SPACE_USED =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_SPACE_USED_COUNT.getName());
  }
}
