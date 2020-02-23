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

import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A class to manage & serve cached pages. This class coordinates various components to respond for
 * thread-safety and enforce cache replacement policies.
 *
 * One of the motivations of creating a client-side cache is from "Improving In-Memory File System
 * Reading Performance by Fine-Grained User-Space Cache Mechanisms" by Gu et al, which illustrates
 * performance benefits for various read workloads. This class also introduces paging as a caching
 * unit.
 *
 * Lock hierarchy in this class: All operations must follow this order to operate on pages:
 * <ol>
 * <li>Acquire corresponding page lock</li>
 * <li>Acquire metastore lock mMetaLock</li>
 * <li>Update metastore</li>
 * <li>Release metastore lock mMetaLock</li>
 * <li>Update the pagestore and evictor</li>
 * <li>Release corresponding page lock</li>
 * </ol>
 */
@ThreadSafe
public class LocalCacheManager implements CacheManager {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheManager.class);

  private static final int LOCK_SIZE = 1024;
  private final long mPageSize;
  private final long mCacheSize;
  private final CacheEvictor mEvictor;
  /** A readwrite lock pool to guard individual pages based on striping. */
  private final ReadWriteLock[] mPageLocks = new ReentrantReadWriteLock[LOCK_SIZE];
  private final PageStore mPageStore;
  /** A readwrite lock to guard metadata operations. */
  private final ReadWriteLock mMetaLock = new ReentrantReadWriteLock();
  @GuardedBy("mMetaLock")
  private final MetaStore mMetaStore;

  /**
   * @param conf the Alluxio configuration
   * @return an instance of {@link LocalCacheManager}
   */
  public static LocalCacheManager create(AlluxioConfiguration conf) throws IOException {
    MetaStore metaStore = MetaStore.create();
    CacheEvictor evictor = CacheEvictor.create(conf);
    PageStoreOptions options = PageStoreOptions.create(conf);
    PageStore pageStore = null;
    try {
      pageStore = PageStore.create(options, false, metaStore, evictor);
    } catch (Exception e) {
      LOG.error("Failed to restore PageStore", e);
    }
    if (pageStore == null) {
      metaStore.reset();
      evictor.reset();
      pageStore = PageStore.create(options);
    }
    return new LocalCacheManager(conf, metaStore, pageStore, evictor);
  }

  /**
   * @param conf the Alluxio configuration
   * @param evictor the eviction strategy to use
   * @param metaStore the meta store manages the metadata
   * @param pageStore the page store manages the cache data
   */
  @VisibleForTesting
  LocalCacheManager(AlluxioConfiguration conf, MetaStore metaStore,
      PageStore pageStore, CacheEvictor evictor) {
    mMetaStore = metaStore;
    mPageStore = pageStore;
    mEvictor = evictor;
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    mCacheSize = pageStore.getCacheSize();
    for (int i = 0; i < LOCK_SIZE; i++) {
      mPageLocks[i] = new ReentrantReadWriteLock();
    }
  }

  /**
   * Gets the lock for a particular page. Note that multiple pages may share the same lock as lock
   * striping is used to reduce resource overhead for locks.
   *
   * @param pageId page identifier
   * @return the corresponding page lock
   */
  private ReadWriteLock getPageLock(PageId pageId) {
    return mPageLocks
        [Math.floorMod((int) (pageId.getFileId().hashCode() + pageId.getPageIndex()), LOCK_SIZE)];
  }

  /**
   * Gets a pair of locks to operate two given pages. One MUST acquire the first lock followed by
   * the second lock.
   *
   * @param pageId page identifier
   * @param pageId2 page identifier
   * @return the corresponding page lock pair
   */
  private Pair<ReadWriteLock, ReadWriteLock> getPageLockPair(PageId pageId, PageId pageId2) {
    if (pageId.getFileId().hashCode() + pageId.getPageIndex()
        < pageId2.getFileId().hashCode() + pageId2.getPageIndex()) {
      return new Pair<>(getPageLock(pageId), getPageLock(pageId2));
    } else {
      return new Pair<>(getPageLock(pageId2), getPageLock(pageId));
    }
  }

  @Override
  public boolean put(PageId pageId, byte[] page) {
    LOG.debug("put({},{} bytes) enters", pageId, page.length);
    PageId victim = null;
    PageInfo victimPageInfo = null;
    boolean enoughSpace;

    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
        if (mMetaStore.hasPage(pageId)) {
          LOG.debug("{} is already inserted before", pageId);
          return false;
        }
        enoughSpace = mMetaStore.bytes() + page.length <= mCacheSize;
        if (enoughSpace) {
          mMetaStore.addPage(pageId, new PageInfo(pageId, page.length));
        } else {
          victim = mEvictor.evict();
        }
      }
      if (enoughSpace) {
        boolean ret = addPage(pageId, page);
        LOG.debug("put({},{} bytes) exits without eviction, success: {}", pageId, page.length, ret);
        return ret;
      }
    }

    Pair<ReadWriteLock, ReadWriteLock> pageLockPair = getPageLockPair(pageId, victim);
    try (LockResource r1 = new LockResource(pageLockPair.getFirst().writeLock());
        LockResource r2 = new LockResource(pageLockPair.getSecond().writeLock())) {
      try (LockResource r3 = new LockResource(mMetaLock.writeLock())) {
        if (mMetaStore.hasPage(pageId)) {
          LOG.debug("{} is already inserted by a racing thread", pageId);
          return false;
        }
        if (!mMetaStore.hasPage(victim)) {
          LOG.debug("{} is already evicted by a racing thread", pageId);
          return false;
        }
        try {
          victimPageInfo = mMetaStore.getPageInfo(victim);
          mMetaStore.removePage(victim);
        } catch (PageNotFoundException e) {
          LOG.error("Page store is missing page {}: {}", victim, e);
          return false;
        }
        enoughSpace = mMetaStore.bytes() + page.length <= mCacheSize;
        if (enoughSpace) {
          mMetaStore.addPage(pageId, new PageInfo(pageId, page.length));
        }
      }
      if (!deletePage(victim, victimPageInfo)) {
        LOG.debug("Failed to evict page: {}", victim);
        return false;
      }
      if (enoughSpace) {
        boolean ret = addPage(pageId, page);
        LOG.debug("put({},{} bytes) exits after evicting ({}), success: {}", pageId, page.length,
            victimPageInfo, ret);
        return ret;
      }
    }
    LOG.debug("put({},{} bytes) fails after evicting ({})", pageId, page.length, victimPageInfo);
    return false;
  }

  @Override
  public ReadableByteChannel get(PageId pageId) {
    return get(pageId, 0);
  }

  @Override
  public ReadableByteChannel get(PageId pageId, int pageOffset) {
    Preconditions.checkArgument(pageOffset <= mPageSize,
        "Read exceeds page boundary: offset=%s size=%s", pageOffset, mPageSize);
    LOG.debug("get({},pageOffset={}) enters", pageId, pageOffset);
    boolean hasPage;
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.readLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.readLock())) {
        hasPage = mMetaStore.hasPage(pageId);
      }
      if (!hasPage) {
        LOG.debug("get({},pageOffset={}) fails due to page not found", pageId, pageOffset);
        return null;
      }
      ReadableByteChannel ret = getPage(pageId, pageOffset);
      LOG.debug("get({},pageOffset={}) exits", pageId, pageOffset);
      return ret;
    }
  }

  @Override
  public boolean delete(PageId pageId) {
    LOG.debug("delete({}) enters", pageId);
    ReadWriteLock pageLock = getPageLock(pageId);
    PageInfo pageInfo;
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r1 = new LockResource(mMetaLock.writeLock())) {
        try {
          pageInfo = mMetaStore.getPageInfo(pageId);
          mMetaStore.removePage(pageId);
        } catch (PageNotFoundException e) {
          LOG.error("Failed to delete page {}: {}", pageId, e);
          return false;
        }
      }
      boolean ret = deletePage(pageId, pageInfo);
      LOG.debug("delete({}) exits, success: {}", pageId, ret);
      return ret;
    }
  }

  @Override
  public void close() throws Exception {
    mPageStore.close();
  }

  /**
   * Attempts to add a page to the page store. The page lock must be acquired before calling this
   * method. The metastore must be updated before calling this method.
   *
   * @param pageId page id
   * @param page page data
   * @return true if successful, false otherwise
   */
  private boolean addPage(PageId pageId, byte[] page) {
    try {
      mPageStore.put(pageId, page);
    } catch (IOException e) {
      LOG.error("Failed to add page {}: {}", pageId, e);
      return false;
    }
    mEvictor.updateOnPut(pageId);
    Metrics.BYTES_WRITTEN_CACHE.inc(page.length);
    return true;
  }

  /**
   * Attempts to delete a page from the page store. The page lock must be acquired before calling
   * this method. The metastore must be updated before calling this method.
   *
   * @param pageId page id
   * @param pageInfo page info
   * @return true if successful, false otherwise
   */
  private boolean deletePage(PageId pageId, PageInfo pageInfo) {
    try {
      mPageStore.delete(pageId, pageInfo.getPageSize());
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to delete page {}: {}", pageId, e);
      return false;
    }
    mEvictor.updateOnDelete(pageId);
    Metrics.BYTES_EVICTED_CACHE.inc(pageInfo.getPageSize());
    return true;
  }

  @Nullable
  private ReadableByteChannel getPage(PageId pageId, int offset) {
    ReadableByteChannel ret;
    try {
      ret = mPageStore.get(pageId, offset);
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to get existing page {}: {}", pageId, e);
      return null;
    }
    mEvictor.updateOnGet(pageId);
    return ret;
  }

  private static final class Metrics {
    /** Bytes written to the cache. */
    private static final Counter BYTES_WRITTEN_CACHE =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_BYTES_WRITTEN_CACHE.getName());
    /** Bytes evicted from the cache. */
    private static final Counter BYTES_EVICTED_CACHE =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_BYTES_EVICTED.getName());
  }
}
