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

import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.resource.LockResource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    PageStore pageStore = PageStore.create(conf);
    try {
      Collection<PageId> pages = pageStore.getPages();
      for (PageId page : pages) {
        metaStore.addPage(page, 0); //fixme
        evictor.updateOnPut(page);
      }
      return new LocalCacheManager(conf, metaStore, pageStore, evictor);
    } catch (Exception e) {
      try {
        pageStore.close();
      } catch (Exception ex) {
        e.addSuppressed(ex);
      }
      throw new IOException("failed to create local cache manager", e);
    }
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
    mCacheSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE);
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
    return mPageLocks[Math.floorMod((int) (pageId.getFileId() + pageId.getPageIndex()), LOCK_SIZE)];
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
    if (pageId.getFileId() + pageId.getPageIndex() < pageId2.getFileId() + pageId2.getPageIndex()) {
      return new Pair<>(getPageLock(pageId), getPageLock(pageId2));
    } else {
      return new Pair<>(getPageLock(pageId2), getPageLock(pageId));
    }
  }

  @Override
  public boolean put(PageId pageId, byte[] page) throws IOException {
    PageId victim = null;
    long victimPageSize = 0;
    boolean enoughSpace;

    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
        if (mMetaStore.hasPage(pageId)) {
          LOG.debug("{} is already inserted before", pageId);
          return false;
        }
        enoughSpace = mPageStore.bytes() + page.length <= mCacheSize;
        if (enoughSpace) {
          mMetaStore.addPage(pageId, page.length);
        } else {
          victim = mEvictor.evict();
          victimPageSize = mMetaStore.getPageSize(victim);
        }
      } catch (PageNotFoundException e) {
        throw new IllegalStateException("we shall not reach here");
      }
      if (enoughSpace) {
        mPageStore.put(pageId, page);
        mEvictor.updateOnPut(pageId);
        return true;
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
          mMetaStore.removePage(victim);
        } catch (PageNotFoundException e) {
          throw new IllegalStateException(
              String.format("Page store is missing page %s.", victim), e);
        }
        enoughSpace = mPageStore.bytes() - victimPageSize + page.length <= mCacheSize;
        if (enoughSpace) {
          mMetaStore.addPage(pageId, page.length);
        }
      }
      try {
        mPageStore.delete(victim, victimPageSize);
        mEvictor.updateOnDelete(victim);
      } catch (PageNotFoundException e) {
        throw new IllegalStateException(String.format("Page store is missing page %s.", victim), e);
      }
      if (enoughSpace) {
        mPageStore.put(pageId, page);
        mEvictor.updateOnPut(pageId);
      }
    }
    return enoughSpace;
  }

  @Override
  public ReadableByteChannel get(PageId pageId) throws IOException {
    return get(pageId, 0);
  }

  @Override
  public ReadableByteChannel get(PageId pageId, int pageOffset)
      throws IOException {
    Preconditions.checkArgument(pageOffset <= mPageSize,
        "Read exceeds page boundary: offset=%s size=%s", pageOffset, mPageSize);
    boolean hasPage;
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.readLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.readLock())) {
        hasPage = mMetaStore.hasPage(pageId);
      }
      if (!hasPage) {
        return null;
      }
      ReadableByteChannel ret = mPageStore.get(pageId, pageOffset);
      mEvictor.updateOnGet(pageId);
      return ret;
    } catch (PageNotFoundException e) {
      throw new IllegalStateException(
          String.format("Page store is missing page %s.", pageId), e);
    }
  }

  @Override
  public void delete(PageId pageId) throws IOException, PageNotFoundException {
    ReadWriteLock pageLock = getPageLock(pageId);
    long pageSize = 0;
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r1 = new LockResource(mMetaLock.writeLock())) {
        pageSize = mMetaStore.getPageSize(pageId);
        mMetaStore.removePage(pageId);
      }
      mPageStore.delete(pageId, pageSize);
      mEvictor.updateOnDelete(pageId);
    }
  }

  @Override
  public void close() throws Exception {
    mPageStore.close();
  }
}
