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

  private static final int LOCK_SIZE = 1024;
  private final long mPageSize;
  private final long mCacheSize;
  private final boolean mAsyncWrite;
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
   * @return whether the restore succeeds or not
   */
  private static boolean restore(
      PageStore pageStore, PageStoreOptions options, MetaStore metaStore) {
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
    MetaStore metaStore = MetaStore.create(CacheEvictor.create(conf));
    PageStoreOptions options = PageStoreOptions.create(conf);
    PageStore pageStore = null;
    boolean restored = false;
    try {
      pageStore = PageStore.create(options, false);
      restored = restore(pageStore, options, metaStore);
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
      pageStore = PageStore.create(options, true);
    }
    return new LocalCacheManager(conf, metaStore, pageStore);
  }

  /**
   * @param conf the Alluxio configuration
   * @param metaStore the meta store manages the metadata
   * @param pageStore the page store manages the cache data
   */
  @VisibleForTesting
  LocalCacheManager(AlluxioConfiguration conf, MetaStore metaStore, PageStore pageStore) {
    mMetaStore = metaStore;
    mPageStore = pageStore;
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
      boolean ok = putInternal(pageId, page);
      LOG.debug("put({},{} bytes) exits: {}", pageId, page.length, ok);
      if (!ok) {
        Metrics.PUT_ERRORS.inc();
      }
      return ok;
    }

    if (!mPendingRequests.add(pageId)) { // already queued
      return false;
    }
    try {
      mAsyncCacheExecutor.submit(() -> {
        try {
          boolean ok = putInternal(pageId, page);
          if (!ok) {
            Metrics.PUT_ERRORS.inc();
          }
        } finally {
          mPendingRequests.remove(pageId);
        }
      });
    } catch (RejectedExecutionException e) { // queue is full, skip
      // RejectedExecutionException may be thrown in extreme cases when the
      // highly concurrent caching workloads. In these cases, return false
      mPendingRequests.remove(pageId);
      Metrics.PUT_ASYNC_REJECTION_ERRORS.inc();
      Metrics.PUT_ERRORS.inc();
      LOG.debug("put({},{} bytes) fails due to full queue", pageId, page.length);
      return false;
    }
    LOG.debug("put({},{} bytes) exits with async write", pageId, page.length);
    return true;
  }

  private boolean putInternal(PageId pageId, byte[] page) {
    LOG.debug("putInternal({},{} bytes) enters", pageId, page.length);
    PageInfo victimPageInfo = null;
    boolean enoughSpace;
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
        if (mMetaStore.hasPage(pageId)) {
          LOG.debug("{} is already inserted before", pageId);
          // TODO(binfan): we should return more informative result in the future
          return true;
        }
        enoughSpace = mMetaStore.bytes() + page.length <= mCacheSize;
        if (enoughSpace) {
          mMetaStore.addPage(pageId, new PageInfo(pageId, page.length));
        } else {
          victimPageInfo = mMetaStore.evict();
          if (victimPageInfo == null) {
            LOG.error("Unable to find page to evict: space used {}, page length {}, cache size {}",
                mMetaStore.bytes(), page.length, mCacheSize);
            Metrics.PUT_EVICTION_ERRORS.inc();
            return false;
          }
        }
      }
      if (enoughSpace) {
        try {
          mPageStore.put(pageId, page);
          Metrics.BYTES_WRITTEN_CACHE.mark(page.length);
          return true;
        } catch (IOException e) {
          undoAddPage(pageId);
          LOG.error("Failed to add page {}: {}", pageId, e);
          Metrics.PUT_STORE_WRITE_ERRORS.inc();
          return false;
        }
      }
    }

    Pair<ReadWriteLock, ReadWriteLock> pageLockPair =
        getPageLockPair(pageId, victimPageInfo.getPageId());
    try (LockResource r1 = new LockResource(pageLockPair.getFirst().writeLock());
        LockResource r2 = new LockResource(pageLockPair.getSecond().writeLock())) {
      // Excise a two-phase commit to evict victim and add new page:
      // phase1: remove victim and add new page in metastore in a critical section protected by
      // metalock. Evictor will be updated inside metastore.
      try (LockResource r3 = new LockResource(mMetaLock.writeLock())) {
        if (mMetaStore.hasPage(pageId)) {
          LOG.debug("{} is already inserted by a racing thread", pageId);
          // TODO(binfan): we should return more informative result in the future
          return true;
        }
        try {
          mMetaStore.removePage(victimPageInfo.getPageId());
        } catch (Exception e) {
          undoAddPage(pageId);
          LOG.error("Page {} is unavailable to evict, likely due to a benign race",
              victimPageInfo.getPageId());
          Metrics.PUT_BENIGN_RACING_ERRORS.inc();
          return false;
        }
        enoughSpace = mMetaStore.bytes() + page.length <= mCacheSize;
        if (enoughSpace) {
          mMetaStore.addPage(pageId, new PageInfo(pageId, page.length));
        }
      }
      // phase2: remove victim and add new page in pagestore
      // Regardless of enoughSpace, delete the victim as it has been removed from the metastore
      PageId victim = victimPageInfo.getPageId();
      try {
        mPageStore.delete(victim);
        Metrics.BYTES_EVICTED_CACHE.mark(victimPageInfo.getPageSize());
        Metrics.PAGES_EVICTED_CACHE.mark();
      } catch (IOException | PageNotFoundException e) {
        if (enoughSpace) {
          // Failed to evict page, remove new page from metastore as there will not be enough space
          undoAddPage(pageId);
        }
        LOG.error("Failed to delete page {}: {}", pageId, e);
        Metrics.PUT_STORE_DELETE_ERRORS.inc();
        return false;
      }
      if (!enoughSpace) {
        Metrics.PUT_EVICTION_ERRORS.inc();
        return false;
      }
      try {
        mPageStore.put(pageId, page);
        Metrics.BYTES_WRITTEN_CACHE.mark(page.length);
        return true;
      } catch (IOException e) {
        // Failed to add page, remove new page from metastoree
        undoAddPage(pageId);
        LOG.error("Failed to add page {}: {}", pageId, e);
        Metrics.PUT_STORE_WRITE_ERRORS.inc();
        return false;
      }
    }
  }

  private void undoAddPage(PageId pageId) {
    try (LockResource r3 = new LockResource(mMetaLock.writeLock())) {
      mMetaStore.removePage(pageId);
    } catch (Exception e) {
      // best effort to remove this page from meta store and ignore the exception
      Metrics.CLEANUP_PUT_ERRORS.inc();
      LOG.error("Failed to undo page add {}", pageId, e);
    }
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
        Metrics.GET_ERRORS.inc();
        Metrics.GET_STORE_READ_ERRORS.inc();
        // something is wrong to read this page, let's remove it from meta store
        try (LockResource r2 = new LockResource(mMetaLock.writeLock())) {
          mMetaStore.removePage(pageId);
        } catch (PageNotFoundException e) {
          // best effort to remove this page from meta store and ignore the exception
          Metrics.CLEANUP_GET_ERRORS.inc();
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
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r1 = new LockResource(mMetaLock.writeLock())) {
        try {
          mMetaStore.removePage(pageId);
        } catch (PageNotFoundException e) {
          LOG.error("Failed to delete page {}: {}", pageId, e);
          Metrics.DELETE_NON_EXISTING_PAGE_ERRORS.inc();
          Metrics.DELETE_ERRORS.inc();
          return false;
        }
      }
      boolean ok = deletePage(pageId);
      LOG.debug("delete({}) exits, success: {}", pageId, ok);
      if (!ok) {
        Metrics.DELETE_STORE_DELETE_ERRORS.inc();
        Metrics.DELETE_ERRORS.inc();
      }
      return ok;
    }
  }

  @Override
  public void close() throws Exception {
    mPageStore.close();
  }

  /**
   * Attempts to delete a page from the page store. The page lock must be acquired before calling
   * this method. The metastore must be updated before calling this method.
   *
   * @param pageId page id
   * @return true if successful, false otherwise
   */
  private boolean deletePage(PageId pageId) {
    try {
      mPageStore.delete(pageId);
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to delete page {}: {}", pageId, e);
      return false;
    }
    return true;
  }

  private int getPage(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
      int bufferOffset) {
    try {
      int ret = mPageStore.get(pageId, pageOffset, buffer, bufferOffset);
      if (ret != bytesToRead) {
        // data read from page store is inconsistent from the metastore
        LOG.error("Failed to read page {}: supposed to read {} bytes, {} bytes actually read",
            pageId, bytesToRead, ret);
        return -1;
      }
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to get existing page {}: {}", pageId, e);
      return -1;
    }
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
    /** Errors when deleting pages due to absence. */
    private static final Counter DELETE_NON_EXISTING_PAGE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS.getName());
    /** Errors when deleting pages due to failed delete in page stores. */
    private static final Counter DELETE_STORE_DELETE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_STORE_DELETE_ERRORS.getName());
    /** Errors when getting pages. */
    private static final Counter GET_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_ERRORS.getName());
    /** Errors when getting pages due to failed read from page stores. */
    private static final Counter GET_STORE_READ_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_STORE_READ_ERRORS.getName());
    /** Errors when cleaning up a failed get operation. */
    private static final Counter CLEANUP_GET_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_CLEANUP_GET_ERRORS.getName());
    /** Errors when cleaning up a failed put operation. */
    private static final Counter CLEANUP_PUT_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_CLEANUP_PUT_ERRORS.getName());
    /** Errors when adding pages. */
    private static final Counter PUT_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_ERRORS.getName());
    /** Errors when adding pages due to failed injection to async write queue. */
    private static final Counter PUT_ASYNC_REJECTION_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_ASYNC_REJECTION_ERRORS.getName());
    /** Errors when adding pages due to failed eviction. */
    private static final Counter PUT_EVICTION_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_EVICTION_ERRORS.getName());
    /** Errors when adding pages due to benign racing eviction. */
    private static final Counter PUT_BENIGN_RACING_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS.getName());
    /** Errors when adding pages due to failed deletes in page store. */
    private static final Counter PUT_STORE_DELETE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_STORE_DELETE_ERRORS.getName());
    /** Errors when adding pages due to failed writes to page store. */
    private static final Counter PUT_STORE_WRITE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_STORE_WRITE_ERRORS.getName());

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
