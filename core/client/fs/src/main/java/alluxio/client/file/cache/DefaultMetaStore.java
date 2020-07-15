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

import alluxio.client.quota.Scope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The default implementation of a metadata store for pages stored in cache. This implementation
 * is not thread safe and requires synchronizations on external callers.
 */
public class DefaultMetaStore implements MetaStore {
  /** A map from PageId to page info. */
  private final Map<PageId, PageInfo> mPageMap = new HashMap<>();
  /** The number of logical bytes used. */
  private final AtomicLong mBytes = new AtomicLong(0);
  /** The number of pages stored. */
  private final AtomicLong mPages = new AtomicLong(0);
  /** Track the number of bytes on each scope. */
  private final Map<Scope, Long> mBytesInScope = new ConcurrentHashMap<>();
  private final boolean mQuotaEnabled;

  /**
   * @param conf configuration
   */
  public DefaultMetaStore(AlluxioConfiguration conf) {
    mQuotaEnabled = conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED);
  }

  @Override
  public boolean hasPage(PageId pageId) {
    return mPageMap.containsKey(pageId);
  }

  @Override
  public void addPage(PageId pageId, PageInfo pageInfo) {
    mPageMap.put(pageId, pageInfo);
    mBytes.addAndGet(pageInfo.getPageSize());
    mPages.incrementAndGet();
    if (mQuotaEnabled) {
      for (Scope scope = pageInfo.getScope(); scope != null; scope = scope.parent()) {
        mBytesInScope.compute(scope,
            (k, v) -> (v == null) ? pageInfo.getPageSize() : v + pageInfo.getPageSize());
      }
    }
  }

  @Override
  public PageInfo getPageInfo(PageId pageId) throws PageNotFoundException {
    if (!mPageMap.containsKey(pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
    return mPageMap.get(pageId);
  }

  @Override
  public void removePage(PageId pageId) throws PageNotFoundException {
    if (!mPageMap.containsKey(pageId)) {
      throw new PageNotFoundException(String.format("Page %s could not be found", pageId));
    }
    PageInfo pageInfo = mPageMap.remove(pageId);
    mBytes.addAndGet(-pageInfo.getPageSize());
    mPages.decrementAndGet();
    if (mQuotaEnabled) {
      for (Scope scope = pageInfo.getScope(); scope != null; scope = scope.parent()) {
        mBytesInScope.computeIfPresent(scope, (k, v) -> v - pageInfo.getPageSize());
      }
    }
  }

  @Override
  public long bytes() {
    return mBytes.get();
  }

  @Override
  public long bytes(Scope scope) {
    if (mQuotaEnabled) {
      return mBytesInScope.get(scope);
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
    mBytes.set(0);
    mPageMap.clear();
    mBytesInScope.clear();
  }
}
