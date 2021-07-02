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
import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.annotation.Nullable;

/**
 * A metastore implementation that tracking usage associated with each cache scope.
 */
public class QuotaMetaStore extends DefaultMetaStore {
  /** Track the number of bytes on each scope. */
  private final Map<CacheScope, Long> mBytesInScope;
  private final Map<CacheScope, CacheEvictor> mCacheEvictors;
  private final Supplier<CacheEvictor> mSupplier;

  /**
   * @param conf configuration
   */
  public QuotaMetaStore(AlluxioConfiguration conf) {
    super(conf);
    mBytesInScope = new ConcurrentHashMap<>();
    mCacheEvictors = new ConcurrentHashMap<>();
    mSupplier = () -> CacheEvictor.create(conf);
  }

  @Override
  public void addPage(PageId pageId, PageInfo pageInfo) {
    super.addPage(pageId, pageInfo);
    for (CacheScope cacheScope = pageInfo.getScope(); cacheScope != CacheScope.GLOBAL; cacheScope =
        cacheScope.parent()) {
      mBytesInScope.compute(cacheScope,
          (k, v) -> (v == null) ? pageInfo.getPageSize() : v + pageInfo.getPageSize());
      CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
      evictor.updateOnPut(pageId);
    }
  }

  @Override
  public PageInfo getPageInfo(PageId pageId) throws PageNotFoundException {
    PageInfo pageInfo = super.getPageInfo(pageId);
    for (CacheScope cacheScope = pageInfo.getScope(); cacheScope != CacheScope.GLOBAL; cacheScope =
        cacheScope.parent()) {
      CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
      evictor.updateOnPut(pageId);
    }
    return pageInfo;
  }

  @Override
  public PageInfo removePage(PageId pageId) throws PageNotFoundException {
    PageInfo pageInfo = super.removePage(pageId);
    for (CacheScope cacheScope = pageInfo.getScope(); cacheScope != CacheScope.GLOBAL; cacheScope =
        cacheScope.parent()) {
      mBytesInScope.computeIfPresent(cacheScope, (k, v) -> v - pageInfo.getPageSize());
      CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
      evictor.updateOnDelete(pageId);
    }
    return pageInfo;
  }

  /**
   * @param cacheScope scope to query
   * @return the total size of pages stored in bytes of the given scope
   */
  public long bytes(CacheScope cacheScope) {
    if (cacheScope == CacheScope.GLOBAL) {
      return bytes();
    }
    return mBytesInScope.computeIfAbsent(cacheScope, k -> 0L);
  }

  @Override
  public void reset() {
    super.reset();
    for (CacheEvictor evictor : mCacheEvictors.values()) {
      evictor.reset();
    }
    mBytesInScope.clear();
  }

  /**
   * @param cacheScope scope to evict
   * @return a page to evict in this scope
   */
  @Nullable
  public PageInfo evict(CacheScope cacheScope) {
    if (cacheScope == CacheScope.GLOBAL) {
      return evict();
    }
    CacheEvictor evictor = mCacheEvictors.computeIfAbsent(cacheScope, k -> mSupplier.get());
    return evictInternal(evictor);
  }
}
