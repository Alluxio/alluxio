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

import static alluxio.client.file.cache.CacheManager.State.NOT_IN_USE;
import static alluxio.client.file.cache.CacheManager.State.READ_ONLY;
import static alluxio.client.file.cache.CacheManager.State.READ_WRITE;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.store.ByteArrayTargetBuffer;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.Pair;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A class to manage & serve cached pages. This class coordinates various components to respond for
 * thread-safety and enforce cache replacement policies.
 *
 * The idea of creating a client-side cache is followed after "Improving In-Memory File System
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
  private final long mCacheSize;
  /** A readwrite lock pool to guard individual pages based on striping. */
  private final ReadWriteLock[] mPageLocks = new ReentrantReadWriteLock[LOCK_SIZE];
  private final List<PageStoreDir> mPageStoreDirs;
  @GuardedBy("PageMetaStore.getLock()")
  private final PageMetaStore mPageMetaStore;
  /** Executor service for execute the init tasks. */
  private final Optional<ExecutorService> mInitService;
  /** Executor service for execute the async cache tasks. */
  private final Optional<ExecutorService> mAsyncCacheExecutor;
  private final ConcurrentHashSet<PageId> mPendingRequests;
  /** State of this cache. */
  private final AtomicReference<CacheManager.State> mState = new AtomicReference<>();
  private final CacheManagerOptions mOptions;

  /**
   * @param options the options of local cache manager
   * @param pageMetaStore the metadata store for local cache
   * @return an instance of {@link LocalCacheManager}
   */
  public static LocalCacheManager create(CacheManagerOptions options,
      PageMetaStore pageMetaStore)
      throws IOException {
    LocalCacheManager manager = new LocalCacheManager(options, pageMetaStore);
    List<PageStoreDir> pageStoreDirs = pageMetaStore.getStoreDirs();
    if (manager.mInitService.isPresent()) {
      manager.mInitService.get().submit(() -> {
        try {
          manager.restoreOrInit(pageStoreDirs);
        } catch (IOException e) {
          LOG.error("Failed to restore LocalCacheManager", e);
        }
      });
    } else {
      manager.restoreOrInit(pageStoreDirs);
    }
    return manager;
  }

  /**
   * @param options the options of local cache manager
   * @param pageMetaStore the meta store manages the metadata
   */
  @VisibleForTesting
  LocalCacheManager(CacheManagerOptions options, PageMetaStore pageMetaStore) {
    mPageMetaStore = pageMetaStore;
    mPageStoreDirs = pageMetaStore.getStoreDirs();
    mOptions = options;
    mCacheSize = mPageStoreDirs.stream().map(PageStoreDir::getCapacityBytes).reduce(0L, Long::sum);
    for (int i = 0; i < LOCK_SIZE; i++) {
      mPageLocks[i] = new ReentrantReadWriteLock(true /* fair ordering */);
    }
    mPendingRequests = new ConcurrentHashSet<>();
    mAsyncCacheExecutor =
        options.isAsyncWriteEnabled()
            ? Optional.of(
              new ThreadPoolExecutor(mOptions.getAsyncWriteThreads(),
                  mOptions.getAsyncWriteThreads(), 60, TimeUnit.SECONDS,
                  new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy()))
            : Optional.empty();
    mInitService =
        options.isAsyncRestoreEnabled() ? Optional.of(Executors.newSingleThreadExecutor()) :
            Optional.empty();
    Metrics.registerGauges(mCacheSize, mPageMetaStore);
    mState.set(READ_ONLY);
    Metrics.STATE.inc();
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

  /**
   * Results of Put.
   */
  enum PutResult {
    BENIGN_RACING,
    INSUFFICIENT_SPACE_EVICTED,
    NO_SPACE_LEFT,
    OK,
    OTHER,
  }

  /**
   * @return which scope to evict a page or null if space is sufficient
   */
  @Nullable
  private CacheScope checkScopeToEvict(int pageSize,
      PageStoreDir pageStoreDir,
      CacheScope scope, CacheQuota quota,
      boolean forcedToEvict) {
    if (mOptions.isQuotaEnabled()) {
      // Check quota usage for each scope
      for (CacheScope currentScope = scope; currentScope != null;
           currentScope = currentScope.parent()) {
        if (((QuotaPageMetaStore) mPageMetaStore).bytes(currentScope) + pageSize
            > quota.getQuota(currentScope)) {
          return currentScope;
        }
      }
    }
    // Check cache space usage
    if (forcedToEvict
        || pageStoreDir.getCachedBytes() + pageSize > pageStoreDir.getCapacityBytes()) {
      return CacheScope.GLOBAL;
    }
    return null;
  }

  @Override
  public boolean put(PageId pageId, ByteBuffer page, CacheContext cacheContext) {
    LOG.debug("put({},{} bytes) enters", pageId, page.remaining());
    if (mState.get() != READ_WRITE) {
      Metrics.PUT_NOT_READY_ERRORS.inc();
      Metrics.PUT_ERRORS.inc();
      return false;
    }
    int originPosition = page.position();
    if (!mOptions.isAsyncWriteEnabled()) {
      boolean ok = putInternal(pageId, page, cacheContext);
      LOG.debug("put({},{} bytes) exits: {}", pageId, page.position() - originPosition, ok);
      if (!ok) {
        Metrics.PUT_ERRORS.inc();
      }
      return ok;
    }

    if (!mPendingRequests.add(pageId)) { // already queued
      return false;
    }
    try {
      mAsyncCacheExecutor.get().submit(() -> {
        try {
          boolean ok = putInternal(pageId, page, cacheContext);
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
      LOG.debug("put({},{} bytes) fails due to full queue", pageId,
          page.position() - originPosition);
      return false;
    }
    LOG.debug("put({},{} bytes) exits with async write", pageId, page.position() - originPosition);
    return true;
  }

  private boolean putInternal(PageId pageId, ByteBuffer page, CacheContext cacheContext) {
    PutResult result = PutResult.OK;
    boolean forcedToEvict = false;
    for (int i = 0; i <= mOptions.getMaxEvictionRetries(); i++) {
      result = putAttempt(pageId, page, cacheContext, forcedToEvict);
      switch (result) {
        case OK:
          return true;
        case BENIGN_RACING:
          // failed put attempt due to a benign race, try again.
        case INSUFFICIENT_SPACE_EVICTED:
          // failed put attempt due to insufficient space, try another time.
          // note that, we only evict one item a time in putAttempt. So it is possible the evicted
          // page is not large enough to cover the space needed by this page. Try again
          continue;
        case NO_SPACE_LEFT:
          // failed put attempt due to "No space left on device" error. This can happen on
          // misconfiguration (e.g., cache capacity is larger than what's available), disk issues,
          // or under-estimation of file system overhead (e.g., writing a file of 10B may result
          // in 512 B on disk). In this case, we need to force data to be evicted in retries,
          // otherwise hitratio may drop due to inability to write new data to cache.
          forcedToEvict = true;
          continue;
        case OTHER:
          // fall through intentionally
        default:
          return false;
      }
    }
    if (result == PutResult.BENIGN_RACING) {
      Metrics.PUT_BENIGN_RACING_ERRORS.inc();
    } else if (result == PutResult.INSUFFICIENT_SPACE_EVICTED) {
      Metrics.PUT_INSUFFICIENT_SPACE_ERRORS.inc();
    }
    return false;
  }

  private PutResult putAttempt(PageId pageId, ByteBuffer page, CacheContext cacheContext,
      boolean forcedToEvict) {
    LOG.debug("putInternal({},{} bytes) enters", pageId, page.remaining());
    PageInfo victimPageInfo = null;
    CacheScope scopeToEvict;
    ReadWriteLock pageLock = getPageLock(pageId);
    PageStoreDir pageStoreDir;
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      try (LockResource r2 = new LockResource(mPageMetaStore.getLock().writeLock())) {
        if (mPageMetaStore.hasPage(pageId)) {
          LOG.debug("{} is already inserted before", pageId);
          // TODO(binfan): we should return more informative result in the future
          return PutResult.OK;
        }
        pageStoreDir = mPageMetaStore.allocate(pageId.getFileId(), page.remaining());
        scopeToEvict = checkScopeToEvict(page.remaining(), pageStoreDir,
            cacheContext.getCacheScope(),
            cacheContext.getCacheQuota(), forcedToEvict);
        if (scopeToEvict == null) {
          addPageToMetaStore(pageId, page, cacheContext, pageStoreDir);
        } else {
          if (mOptions.isQuotaEnabled()) {
            victimPageInfo =
                ((QuotaPageMetaStore) mPageMetaStore).evict(scopeToEvict, pageStoreDir);
          } else {
            victimPageInfo = mPageMetaStore.evict(pageStoreDir);
          }
          if (victimPageInfo == null) {
            LOG.error("Unable to find page to evict: space used {}, page length {}, cache size {}",
                mPageMetaStore.bytes(), page.remaining(), mCacheSize);
            Metrics.PUT_EVICTION_ERRORS.inc();
            return PutResult.OTHER;
          }
        }
      }
      if (scopeToEvict == null) {
        try {
          int bytesToWrite = page.remaining();
          pageStoreDir.getPageStore().put(pageId, page, cacheContext.isTemporary());
          // Bytes written to the cache
          MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_WRITTEN_CACHE.getName())
              .mark(bytesToWrite);
          return PutResult.OK;
        } catch (ResourceExhaustedException e) {
          undoAddPage(pageId);
          LOG.error("Failed to add page {} to pageStore", pageId, e);
          Metrics.PUT_STORE_WRITE_NO_SPACE_ERRORS.inc();
          return PutResult.NO_SPACE_LEFT;
        } catch (IOException e) {
          undoAddPage(pageId);
          LOG.error("Failed to add page {} to pageStore", pageId, e);
          Metrics.PUT_STORE_WRITE_ERRORS.inc();
          return PutResult.OTHER;
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
      try (LockResource r3 = new LockResource(mPageMetaStore.getLock().writeLock())) {
        if (mPageMetaStore.hasPage(pageId)) {
          return PutResult.OK;
        }
        try {
          mPageMetaStore.removePage(victimPageInfo.getPageId());
        } catch (PageNotFoundException e) {
          LOG.debug("Page {} is unavailable to evict, likely due to a benign race",
              victimPageInfo.getPageId());
          return PutResult.BENIGN_RACING;
        }
        // Check if we are able to insert page after evicting victim page
        scopeToEvict = checkScopeToEvict(page.remaining(), pageStoreDir,
            cacheContext.getCacheScope(), cacheContext.getCacheQuota(), false);
        if (scopeToEvict == null) {
          addPageToMetaStore(pageId, page, cacheContext, pageStoreDir);
        }
      }
      // phase2: remove victim and add new page in pagestore
      // Regardless of enoughSpace, delete the victim as it has been removed from the metastore
      PageId victim = victimPageInfo.getPageId();
      try {
        pageStoreDir.getPageStore().delete(victim);
        // Bytes evicted from the cache
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_EVICTED.getName())
            .mark(victimPageInfo.getPageSize());
        // Errors when adding pages
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_PAGES_EVICTED.getName()).mark();
      } catch (IOException | PageNotFoundException e) {
        if (scopeToEvict == null) {
          // Failed to evict page, remove new page from metastore as there will not be enough space
          undoAddPage(pageId);
        }
        if (e instanceof PageNotFoundException) {
          //The victim page got deleted by other thread, likely due to a benign racing. Will retry.
          return PutResult.BENIGN_RACING;
        }
        LOG.error("Failed to delete page {} from pageStore", pageId, e);
        Metrics.PUT_STORE_DELETE_ERRORS.inc();
        return PutResult.OTHER;
      }
      if (scopeToEvict != null) {
        return PutResult.INSUFFICIENT_SPACE_EVICTED;
      }
      try {
        int bytesToWrite = page.remaining();
        pageStoreDir.getPageStore().put(pageId, page, cacheContext.isTemporary());
        // Bytes written to the cache
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_WRITTEN_CACHE.getName())
            .mark(bytesToWrite);
        return PutResult.OK;
      } catch (ResourceExhaustedException e) {
        undoAddPage(pageId);
        LOG.error("Failed to add page {} to pageStore", pageId, e);
        Metrics.PUT_STORE_WRITE_NO_SPACE_ERRORS.inc();
        return PutResult.NO_SPACE_LEFT;
      } catch (IOException e) {
        // Failed to add page, remove new page from metastoree
        undoAddPage(pageId);
        LOG.error("Failed to add page {} to pageStore", pageId, e);
        Metrics.PUT_STORE_WRITE_ERRORS.inc();
        return PutResult.OTHER;
      }
    }
  }

  private void addPageToMetaStore(PageId pageId, ByteBuffer page, CacheContext cacheContext,
      PageStoreDir pageStoreDir) {
    PageInfo pageInfo =
        new PageInfo(pageId, page.remaining(), cacheContext.getCacheScope(), pageStoreDir);
    if (cacheContext.isTemporary()) {
      mPageMetaStore.addTempPage(pageId, pageInfo);
    } else {
      mPageMetaStore.addPage(pageId, pageInfo);
    }
  }

  private void undoAddPage(PageId pageId) {
    try (LockResource r3 = new LockResource(mPageMetaStore.getLock().writeLock())) {
      mPageMetaStore.removePage(pageId);
    } catch (Exception e) {
      // best effort to remove this page from meta store and ignore the exception
      Metrics.CLEANUP_PUT_ERRORS.inc();
      LOG.error("Failed to undo page add {}", pageId, e);
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, PageReadTargetBuffer buffer,
      CacheContext cacheContext) {
    Preconditions.checkArgument(pageOffset <= mOptions.getPageSize(),
        "Read exceeds page boundary: offset=%s size=%s", pageOffset, mOptions.getPageSize());
    Preconditions.checkArgument(bytesToRead <= buffer.remaining(),
        "buffer does not have enough space: bufferRemaining=%s bytesToRead=%s",
        buffer.remaining(), bytesToRead);
    LOG.debug("get({},pageOffset={}) enters", pageId, pageOffset);
    if (mState.get() == NOT_IN_USE) {
      Metrics.GET_NOT_READY_ERRORS.inc();
      Metrics.GET_ERRORS.inc();
      return -1;
    }
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.readLock())) {
      PageInfo pageInfo;
      try (LockResource r2 = new LockResource(mPageMetaStore.getLock().readLock())) {
        pageInfo = mPageMetaStore.getPageInfo(pageId); //check if page exists and refresh LRU items
      } catch (PageNotFoundException e) {
        LOG.debug("get({},pageOffset={}) fails due to page not found", pageId, pageOffset);
        return 0;
      }
      int bytesRead =
          getPage(pageInfo, pageOffset, bytesToRead, buffer, cacheContext);
      if (bytesRead <= 0) {
        Metrics.GET_ERRORS.inc();
        Metrics.GET_STORE_READ_ERRORS.inc();
        // something is wrong to read this page, let's remove it from meta store
        try (LockResource r2 = new LockResource(mPageMetaStore.getLock().writeLock())) {
          mPageMetaStore.removePage(pageId);
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

  /**
   * delete the specified page.
   * @param pageId page identifier
   * @param cacheContext cache context
   * @return whether the page is deleted successfully or not
   */
  public boolean delete(PageId pageId, CacheContext cacheContext) {
    if (!cacheContext.isTemporary()) {
      return delete(pageId);
    }
    // TODO(JiamingMai): implement this method
    if (mState.get() != READ_WRITE) {
      Metrics.DELETE_NOT_READY_ERRORS.inc();
      Metrics.DELETE_ERRORS.inc();
      return false;
    }
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      PageInfo pageInfo;
      try (LockResource r1 = new LockResource(mPageMetaStore.getLock().writeLock())) {
        try {
          pageInfo = mPageMetaStore.removeTempPage(pageId, cacheContext);
        } catch (PageNotFoundException e) {
          LOG.error("Failed to delete page {} from metaStore ", pageId, e);
          Metrics.DELETE_NON_EXISTING_PAGE_ERRORS.inc();
          Metrics.DELETE_ERRORS.inc();
          return false;
        }
      }
      boolean ok = deletePage(pageInfo, cacheContext.isTemporary());
      LOG.debug("delete({}) exits, success: {}", pageId, ok);
      if (!ok) {
        Metrics.DELETE_STORE_DELETE_ERRORS.inc();
        Metrics.DELETE_ERRORS.inc();
      }
      return ok;
    }
  }

  @Override
  public boolean delete(PageId pageId) {
    LOG.debug("delete({}) enters", pageId);
    if (mState.get() != READ_WRITE) {
      Metrics.DELETE_NOT_READY_ERRORS.inc();
      Metrics.DELETE_ERRORS.inc();
      return false;
    }
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      PageInfo pageInfo;
      try (LockResource r1 = new LockResource(mPageMetaStore.getLock().writeLock())) {
        try {
          pageInfo = mPageMetaStore.removePage(pageId);
        } catch (PageNotFoundException e) {
          LOG.error("Failed to delete page {} from metaStore ", pageId, e);
          Metrics.DELETE_NON_EXISTING_PAGE_ERRORS.inc();
          Metrics.DELETE_ERRORS.inc();
          return false;
        }
      }
      boolean ok = deletePage(pageInfo, false);
      LOG.debug("delete({}) exits, success: {}", pageId, ok);
      if (!ok) {
        Metrics.DELETE_STORE_DELETE_ERRORS.inc();
        Metrics.DELETE_ERRORS.inc();
      }
      return ok;
    }
  }

  @Override
  public State state() {
    return mState.get();
  }

  @Override
  public boolean append(PageId pageId, int appendAt, byte[] page, CacheContext cacheContext) {
    if (mState.get() != READ_WRITE) {
      Metrics.PUT_NOT_READY_ERRORS.inc();
      Metrics.PUT_ERRORS.inc();
      return false;
    }
    if (appendAt > 0) {
      byte[] newPage = new byte[appendAt + page.length];
      int readBytes = get(pageId, 0, appendAt,
          new ByteArrayTargetBuffer(newPage, 0),  cacheContext);
      boolean success = delete(pageId, cacheContext);
      LOG.debug("delete pageId: " + pageId
          + ", appendAt: " + appendAt + ", readBytes: " + readBytes);
      System.arraycopy(page, 0, newPage, appendAt, page.length);
      return put(pageId, newPage, cacheContext);
    }
    return put(pageId, page, cacheContext);
  }

  /**
   * Restores a page store at the configured location, updating meta store accordingly.
   * If restore process fails, cleanup the location and create a new page store.
   * This method is synchronized to ensure only one thread can enter and operate.
   * @param pageStoreDirs
   */
  private void restoreOrInit(List<PageStoreDir> pageStoreDirs) throws IOException {
    Preconditions.checkState(mState.get() == READ_ONLY);
    for (PageStoreDir pageStoreDir : pageStoreDirs) {
      if (!restore(pageStoreDir)) {
        try (LockResource r = new LockResource(mPageMetaStore.getLock().writeLock())) {
          mPageMetaStore.reset();
        }
        try {
          pageStoreDir.reset();
        } catch (IOException e) {
          LOG.error("Cache is in NOT_IN_USE.");
          mState.set(NOT_IN_USE);
          Metrics.STATE.dec();
          throw e;
        }
      }
    }
    LOG.info("Cache is in READ_WRITE.");
    mState.set(READ_WRITE);
    Metrics.STATE.inc();
  }

  private boolean restore(PageStoreDir pageStoreDir) {
    LOG.info("Restoring PageStoreDir ({})", pageStoreDir.getRootPath());
    if (!Files.exists(pageStoreDir.getRootPath())) {
      LOG.error("Failed to restore PageStore: Directory {} does not exist",
          pageStoreDir.getRootPath());
      return false;
    }
    try {
      pageStoreDir.scanPages(pageInfo -> {
        addPageToDir(pageStoreDir, pageInfo.get());
      });
    } catch (IOException | RuntimeException e) {
      LOG.error("Failed to restore PageStore", e);
      return false;
    }
    LOG.info("PageStore ({}) restored with {} pages ({} bytes), "
            + "discarded {} pages ({} bytes)",
        pageStoreDir.getRootPath(), mPageMetaStore.numPages(), mPageMetaStore.bytes(),
        Metrics.PAGE_DISCARDED.getCount(), Metrics.BYTE_DISCARDED);
    return true;
  }

  private void addPageToDir(PageStoreDir pageStoreDir, PageInfo pageInfo) {
    PageId pageId = pageInfo.getPageId();
    ReadWriteLock pageLock = getPageLock(pageId);
    try (LockResource r = new LockResource(pageLock.writeLock())) {
      boolean enoughSpace;
      try (LockResource r2 = new LockResource(mPageMetaStore.getLock().writeLock())) {
        enoughSpace = pageStoreDir.getCachedBytes() + pageInfo.getPageSize()
            <= pageStoreDir.getCapacityBytes();
        if (enoughSpace) {
          mPageMetaStore.addPage(pageId, pageInfo);
        }
      }
      if (!enoughSpace) {
        try {
          pageStoreDir.getPageStore().delete(pageId);
        } catch (IOException | PageNotFoundException e) {
          throw new RuntimeException("Failed to delete page", e);
        }
        Metrics.PAGE_DISCARDED.inc();
        Metrics.BYTE_DISCARDED.inc(pageInfo.getPageSize());
      }
    }
  }

  @Override
  public List<PageId> getCachedPageIdsByFileId(String fileId, long fileLength) {
    //ceiling round the result
    int numOfPages = (int) ((fileLength - 1) / mOptions.getPageSize()) + 1;
    List<PageId> pageIds = new ArrayList<>(numOfPages);
    try (LockResource r = new LockResource(mPageMetaStore.getLock().readLock())) {
      for (long pageIndex = 0; pageIndex < numOfPages; pageIndex++) {
        PageId pageId = new PageId(fileId, pageIndex);
        if (mPageMetaStore.hasPage(pageId)) {
          pageIds.add(pageId);
        }
      }
    }
    return pageIds;
  }

  @Override
  public void close() throws Exception {
    for (PageStoreDir pageStoreDir: mPageStoreDirs) {
      pageStoreDir.close();
    }
    mPageMetaStore.reset();
    mInitService.ifPresent(ExecutorService::shutdownNow);
    mAsyncCacheExecutor.ifPresent(ExecutorService::shutdownNow);
  }

  /**
   * Attempts to delete a page from the page store. The page lock must be acquired before calling
   * this method. The metastore must be updated before calling this method.
   *
   * @param pageInfo page info
   * @return true if successful, false otherwise
   */
  private boolean deletePage(PageInfo pageInfo, boolean isTemporary) {
    try {
      pageInfo.getLocalCacheDir().getPageStore().delete(pageInfo.getPageId(), isTemporary);
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to delete page {} from pageStore", pageInfo.getPageId(), e);
      return false;
    }
    return true;
  }

  private int getPage(PageInfo pageInfo, int pageOffset, int bytesToRead,
      PageReadTargetBuffer target, CacheContext cacheContext) {
    try {
      int ret = pageInfo.getLocalCacheDir().getPageStore()
          .get(pageInfo.getPageId(), pageOffset, bytesToRead, target,
              cacheContext.isTemporary());
      if (ret != bytesToRead) {
        // data read from page store is inconsistent from the metastore
        LOG.error("Failed to read page {}: supposed to read {} bytes, {} bytes actually read",
            pageInfo.getPageId(), bytesToRead, ret);
        return -1;
      }
    } catch (IOException | PageNotFoundException e) {
      LOG.error("Failed to get existing page {} from pageStore", pageInfo.getPageId(), e);
      return -1;
    }
    return bytesToRead;
  }

  private static final class Metrics {
    // Note that only counter/guage can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
    /** Total number of bytes discarded when restoring the page store. */
    private static final Counter BYTE_DISCARDED =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_BYTES_DISCARDED.getName());
    /** Errors when cleaning up a failed get operation. */
    private static final Counter CLEANUP_GET_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_CLEANUP_GET_ERRORS.getName());
    /** Errors when cleaning up a failed put operation. */
    private static final Counter CLEANUP_PUT_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_CLEANUP_PUT_ERRORS.getName());
    /** Errors when deleting pages. */
    private static final Counter DELETE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_ERRORS.getName());
    /** Errors when deleting pages due to absence. */
    private static final Counter DELETE_NON_EXISTING_PAGE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS.getName());
    /** Errors when cache is not ready to delete pages. */
    private static final Counter DELETE_NOT_READY_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_NOT_READY_ERRORS.getName());
    /** Errors when deleting pages due to failed delete in page stores. */
    private static final Counter DELETE_STORE_DELETE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_FROM_STORE_ERRORS.getName());
    /** Errors when getting pages. */
    private static final Counter GET_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_ERRORS.getName());
    /** Errors when cache is not ready to get pages. */
    private static final Counter GET_NOT_READY_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_NOT_READY_ERRORS.getName());
    /** Errors when getting pages due to failed read from page stores. */
    private static final Counter GET_STORE_READ_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_STORE_READ_ERRORS.getName());
    /** Total number of pages discarded when restoring the page store. */
    private static final Counter PAGE_DISCARDED =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PAGES_DISCARDED.getName());
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
    /** Errors when adding pages due to insufficient space made after eviction. */
    private static final Counter PUT_INSUFFICIENT_SPACE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS.getName());
    /** Errors when cache is not ready to add pages. */
    private static final Counter PUT_NOT_READY_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_NOT_READY_ERRORS.getName());
    /** Errors when adding pages due to failed deletes in page store. */
    private static final Counter PUT_STORE_DELETE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_STORE_DELETE_ERRORS.getName());
    /** Errors when adding pages due to failed writes to page store. */
    private static final Counter PUT_STORE_WRITE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_STORE_WRITE_ERRORS.getName());
    /** Errors when adding pages due to failed writes but before reaching cache capacity. */
    private static final Counter PUT_STORE_WRITE_NO_SPACE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_STORE_WRITE_NO_SPACE_ERRORS.getName());
    /** State of the cache. */
    private static final Counter STATE =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_STATE.getName());

    private static void registerGauges(long cacheSize, PageMetaStore pageMetaStore) {
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SPACE_AVAILABLE.getName()),
          () -> cacheSize - pageMetaStore.bytes());
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLIENT_CACHE_SPACE_USED.getName()),
          pageMetaStore::bytes);
    }
  }
}
