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
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

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

  private static final int LOCK_SIZE = 2048;
  private final long mPageSize;
  private final long mCacheSize;
  private final boolean mAsyncWrite;
  private final CacheEvictor mEvictor;
  /** A readwrite lock pool to guard individual pages based on striping. */
  private final ReadWriteLock[] mPageLocks = new ReentrantReadWriteLock[LOCK_SIZE];
  private final PageStore mPageStore;
  /** A readwrite lock to guard metadata operations. */
  private final ReadWriteLock mMetaLock = new ReentrantReadWriteLock();
  @GuardedBy("mMetaLock")
  private final MetaStore mMetaStore;
  /** Executor service for execute the async cache tasks. */
  private final ExecutorService mAsyncCacheExecutor;
  private final ConcurrentHashSet<PageId> mPendingRequests;

  /**
   * Restores a page store a the configured location, updating meta store and evictor.
   *
   * @param pageStore page store
   * @param options page store options
   * @param metaStore meta store
   * @param evictor evictor
   * @return whether the restore succeeds or not
   */
  private static boolean restore(PageStore pageStore, PageStoreOptions options, MetaStore metaStore,
      CacheEvictor evictor) {
    LOG.info("Attempt to restore PageStore with {}", options);
    Path rootDir = Paths.get(options.getRootDir());
    if (!Files.exists(rootDir)) {
      LOG.error("Failed to restore PageStore: Directory {} does not exist", rootDir);
      return false;
    }
    try (Stream<PageInfo> stream = pageStore.getPages()) {
      Iterator<PageInfo> iterator = stream.iterator();
      while (iterator.hasNext()) {
        PageInfo pageInfo = iterator.next();
        if (pageInfo == null) {
          LOG.error("Invalid page info");
          return false;
        }
        metaStore.addPage(pageInfo.getPageId(), pageInfo);
        evictor.updateOnPut(pageInfo.getPageId());
        if (metaStore.bytes() > pageStore.getCacheSize()) {
          LOG.error("Loaded pages exceed cache capacity ({} bytes)", pageStore.getCacheSize());
          return false;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to restore PageStore", e);
      return false;
    }
    LOG.info("Restored PageStore with {} existing pages and {} bytes", metaStore.pages(),
        metaStore.bytes());
    return true;
  }

  /**
   * @param conf the Alluxio configuration
   * @return an instance of {@link LocalCacheManager}
   */
  public static LocalCacheManager create(AlluxioConfiguration conf) throws IOException {
    MetaStore metaStore = MetaStore.create();
    CacheEvictor evictor = CacheEvictor.create(conf);
    PageStoreOptions options = PageStoreOptions.create(conf);
    PageStore pageStore = null;
    boolean restored = false;
    try {
      pageStore = PageStore.create(options, false);
      restored = restore(pageStore, options, metaStore, evictor);
    } catch (Exception e) {
      LOG.error("Failed to restore PageStore", e);
    }
    if (!restored) {
      if (pageStore != null) {
        try {
          pageStore.close();
        } catch (Exception e) {
          LOG.error("Failed to close PageStore", e);
        }
      }
      metaStore.reset();
      evictor.reset();
      pageStore = PageStore.create(options, true);
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
  LocalCacheManager(AlluxioConfiguration conf, MetaStore metaStore, PageStore pageStore,
      CacheEvictor evictor) {
    mMetaStore = metaStore;
    mPageStore = pageStore;
    mEvictor = evictor;
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    mAsyncWrite = conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED);
    mCacheSize = pageStore.getCacheSize();
    for (int i = 0; i < LOCK_SIZE; i++) {
      mPageLocks[i] = new ReentrantReadWriteLock(true /* fair ordering */);
    }
    mPendingRequests = new ConcurrentHashSet<>();
    mAsyncCacheExecutor =
        mAsyncWrite
            ? new ThreadPoolExecutor(conf.getInt(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_THREADS),
                conf.getInt(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_THREADS), 60,
                TimeUnit.SECONDS, new SynchronousQueue<>())
            : null;
    Metrics.registerGauges(mCacheSize, mMetaStore);
  }

  /**
   * @param pageId page identifier
   * @return the page lock id
   */
  private int getPageLockId(PageId pageId) {
    return Math.floorMod((int) (pageId.getFileId().hashCode() + pageId.getPageIndex()), LOCK_SIZE);
  }

  /**
   * Gets the lock for a particular page. Note that multiple pages may share the same lock as lock
   * striping is used to reduce resource overhead for locks.
   *
   * @param pageId page identifier
   * @return the corresponding page lock
   */
  private ReadWriteLock getPageLock(PageId pageId) {
    return mPageLocks[getPageLockId(pageId)];
  }

  /**
   * Gets a pair of locks to operate two given pages. One MUST acquire the first lock followed by
   * the second lock.
   *
   * @param pageId1 first page identifier
   * @param pageId2 second page identifier
   * @return the corresponding page lock pair
   */
  private Pair<ReadWriteLock, ReadWriteLock> getPageLockPair(PageId pageId1, PageId pageId2) {
    int lockId1 = getPageLockId(pageId1);
    int lockId2 = getPageLockId(pageId2);
    if (lockId1 < lockId2) {
      return new Pair<>(mPageLocks[lockId1], mPageLocks[lockId2]);
    } else {
      return new Pair<>(mPageLocks[lockId2], mPageLocks[lockId1]);
    }
  }

  @Override
  public boolean put(PageId pageId, byte[] page) {
    LOG.debug("put({},{} bytes) enters", pageId, page.length);
    if (!mAsyncWrite) {
      boolean inserted = putInternal(pageId, page);
      LOG.debug("put({},{} bytes) exits: {}", pageId, page.length, inserted);
      return inserted;
    }

    if (!mPendingRequests.add(pageId)) { // already queued
      return false;
    }
    try {
      mAsyncCacheExecutor.submit(() -> {
        try {
          putInternal(pageId, page);
        } finally {
          mPendingRequests.remove(pageId);
        }
      });
    } catch (RejectedExecutionException e) { // queue is full, skip
      // RejectedExecutionException may be thrown in extreme cases when the
      // highly concurrent caching workloads. In these cases, return false
      mPendingRequests.remove(pageId);
      Metrics.PUT_ERRORS.inc();
      LOG.debug("put({},{} bytes) fails due to full queue", pageId, page.length);
      return false;
    }
    LOG.debug("put({},{} bytes) exits with async write", pageId, page.length);
    return true;
  }

  private boolean putInternal(PageId pageId, byte[] page) {
    LOG.debug("putInternal({},{} bytes) enters", pageId, page.length);
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
        if (!ret) {
          // something is wrong to add this page, let's remove it from meta store
          try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
            mMetaStore.removePage(pageId);
          } catch (PageNotFoundException e) {
            // best effort to remove this page from meta store and ignore the exception
            Metrics.PUT_FAILED_WRITE_ERRORS.inc();
          }
        }
        LOG.debug("Add page ({},{} bytes) without eviction: {}", pageId, page.length, ret);
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
        if (!ret) {
          // something is wrong to add this page, let's remove it from meta store
          try (LockResource r3 = new LockResource(mMetaLock.writeLock())) {
            mMetaStore.removePage(pageId);
          } catch (PageNotFoundException e) {
            // best effort to remove this page from meta store and ignore the exception
            Metrics.PUT_FAILED_WRITE_ERRORS.inc();
          }
        }
        LOG.debug("Add page ({},{} bytes) after evicting ({}), success: {}", pageId, page.length,
            victimPageInfo, ret);
        return ret;
      }
    }
    LOG.debug("putInternal({},{} bytes) fails after evicting ({})", pageId, page.length,
        victimPageInfo);
    return false;
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
      int offsetInBuffer) {
    Preconditions.checkArgument(pageOffset <= mPageSize,
        "Read exceeds page boundary: offset=%s size=%s", pageOffset, mPageSize);
    Preconditions.checkArgument(bytesToRead <= buffer.length - offsetInBuffer,
        "buffer does not have enough space: bufferLength=%s offsetInBuffer=%s bytesToRead=%s",
        buffer.length, offsetInBuffer, bytesToRead);
    LOG.debug("get({},pageOffset={}) enters", pageId, pageOffset);
    boolean hasPage;
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.readLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.readLock())) {
        hasPage = mMetaStore.hasPage(pageId);
      }
      if (!hasPage) {
        LOG.debug("get({},pageOffset={}) fails due to page not found", pageId, pageOffset);
        return 0;
      }
      int bytesRead = getPage(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer);
      if (bytesRead <= 0) {
        // something is wrong to read this page, let's remove it from meta store
        try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
          mMetaStore.removePage(pageId);
        } catch (PageNotFoundException e) {
          // best effort to remove this page from meta store and ignore the exception
          Metrics.GET_ERRORS_FAILED_READ.inc();
        }
        return -1;
      }
      LOG.debug("get({},pageOffset={}) exits", pageId, pageOffset);
      return bytesRead;
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
          Metrics.DELETE_ERRORS.inc();
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
      Metrics.PUT_ERRORS.inc();
      return false;
    }
    mEvictor.updateOnPut(pageId);
    Metrics.BYTES_WRITTEN_CACHE.mark(page.length);
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
      mPageStore.delete(pageId);
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to delete page {}: {}", pageId, e);
      Metrics.DELETE_ERRORS.inc();
      return false;
    }
    mEvictor.updateOnDelete(pageId);
    Metrics.BYTES_EVICTED_CACHE.mark(pageInfo.getPageSize());
    Metrics.PAGES_EVICTED_CACHE.mark();
    return true;
  }

  private int getPage(PageId pageId, int offsetInPage, int bytesToRead, byte[] buffer,
      int offsetInBuffer) {
    try {
      int bytesRead = mPageStore.get(pageId, offsetInPage, bytesToRead, buffer, offsetInBuffer);
      if (bytesRead != bytesToRead) {
        // data read from page store is inconsistent from the metastore
        Metrics.GET_ERRORS_FAILED_READ.inc();
        throw new IOException(String.format(
            "Failed to read page {}: supposed to read {} bytes, {} bytes actually read",
            pageId, bytesToRead, bytesRead));
      }
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to get existing page {}: {}", pageId, e);
      Metrics.GET_ERRORS.inc();
      return -1;
    }
    mEvictor.updateOnGet(pageId);
    return bytesToRead;
  }

  private static final class Metrics {
    /** Bytes written to the cache. */
    private static final Meter BYTES_WRITTEN_CACHE =
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_WRITTEN_CACHE.getName());
    /** Bytes evicted from the cache. */
    private static final Meter BYTES_EVICTED_CACHE =
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_EVICTED.getName());
    /** Pages evicted from the cache. */
    private static final Meter PAGES_EVICTED_CACHE =
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_PAGES_EVICTED.getName());
    /** Errors when deleting pages. */
    private static final Counter DELETE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_ERRORS.getName());
    /** Errors when getting pages. */
    private static final Counter GET_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_ERRORS.getName());
    /** Errors when getting pages due to failed reads from cache storage. */
    private static final Counter GET_ERRORS_FAILED_READ =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_FAILED_READ_ERRORS.getName());
    /** Errors when adding pages. */
    private static final Counter PUT_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_ERRORS.getName());
    /** Errors when adding pages due to failed writes to cache storage. */
    private static final Counter PUT_FAILED_WRITE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_FAILED_WRITE_ERRORS.getName());

    private static void registerGauges(long cacheSize, MetaStore metaStore) {
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SPACE_AVAILABLE.getName()),
          () -> cacheSize - metaStore.bytes());
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SPACE_USED.getName()),
          metaStore::bytes);
    }
  }
}
