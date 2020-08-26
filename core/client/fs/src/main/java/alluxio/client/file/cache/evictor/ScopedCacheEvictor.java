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

package alluxio.client.file.cache.evictor;

import alluxio.client.file.cache.CacheEvictor;
import alluxio.client.file.cache.MetaStore;
import alluxio.client.file.cache.PageId;
import alluxio.client.quota.Scope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.PageNotFoundException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A cache eviction policy taking scope into account.
 */
@ThreadSafe
public class ScopedCacheEvictor implements CacheEvictor {
  private final Map<Scope, CacheEvictor> mCacheEvictors = new ConcurrentHashMap<>();
  private final MetaStore mMetaStore;
  private final Supplier<CacheEvictor> mSupplier;

  /**
   * Required constructor.
   *
   * @param conf Alluxio configuration
   * @param metaStore meta store
   */
  public ScopedCacheEvictor(AlluxioConfiguration conf, MetaStore metaStore) {
    mMetaStore = metaStore;
    mSupplier = () -> CacheEvictor.create(conf);
  }

  @Override
  public void updateOnGet(PageId pageId) {
    try {
      for (Scope scope = mMetaStore.getPageInfo(pageId).getScope(); scope != null;
          scope = scope.parent()) {
        CacheEvictor evictor = mCacheEvictors.computeIfAbsent(scope, k -> mSupplier.get());
        evictor.updateOnGet(pageId);
      }
    } catch (PageNotFoundException e) {
      // ignore
    }
  }

  @Override
  public void updateOnPut(PageId pageId) {
    try {
      for (Scope scope = mMetaStore.getPageInfo(pageId).getScope(); scope != null;
           scope = scope.parent()) {
        CacheEvictor evictor = mCacheEvictors.computeIfAbsent(scope, k -> mSupplier.get());
        evictor.updateOnPut(pageId);
      }
    } catch (PageNotFoundException e) {
      // ignore
    }
  }

  @Override
  public void updateOnDelete(PageId pageId) {
    try {
      for (Scope scope = mMetaStore.getPageInfo(pageId).getScope(); scope != null;
           scope = scope.parent()) {
        CacheEvictor evictor = mCacheEvictors.computeIfAbsent(scope, k -> mSupplier.get());
        evictor.updateOnDelete(pageId);
      }
    } catch (PageNotFoundException e) {
      // ignore
    }
  }

  @Nullable
  @Override
  public PageId evict() {
    return evict(Scope.GLOBAL);
  }

  /**
   * @param scope which scope to evict a page
   * @return a page to evict or null if no page available to evict
   */
  @Nullable
  public PageId evict(Scope scope) {
    CacheEvictor cache = mCacheEvictors.computeIfAbsent(scope, k -> mSupplier.get());
    return cache.evict();
  }

  @Override
  public void reset() {
    for (CacheEvictor evictor : mCacheEvictors.values()) {
      evictor.reset();
    }
  }
}
