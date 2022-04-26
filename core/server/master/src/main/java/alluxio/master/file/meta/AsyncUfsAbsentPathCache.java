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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This is a {@link UfsAbsentPathCache} which implements asynchronous addition and removal to the
 * cache, since the processing of the path may be slow.
 */
@ThreadSafe
public final class AsyncUfsAbsentPathCache implements UfsAbsentPathCache {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncUfsAbsentPathCache.class);
  /** Number of seconds to keep threads alive. */
  private static final int THREAD_KEEP_ALIVE_SECONDS = 60;
  /** Number of paths to cache. */
  private static final int MAX_PATHS =
      ServerConfiguration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY);

  /** The mount table. */
  private final MountTable mMountTable;
  /** Paths currently being processed. This is used to prevent duplicate processing. */
  private final ConcurrentHashMap<String, PathLock> mCurrentPaths;
  /** Cache of paths which are absent in the ufs, maps an alluxio path to a Pair
   *  which is the sync time and the mount id.
   */
  private final Cache<String, Pair<Long, Long>> mCache;
  /** A thread pool for the async tasks. */
  private final ThreadPoolExecutor mPool;
  /** Number of threads for the async pool. */
  private final int mThreads;

  /**
   * Creates a new instance of {@link AsyncUfsAbsentPathCache}.
   *
   * @param mountTable the mount table
   * @param numThreads the maximum number of threads for the async thread pool
   */
  public AsyncUfsAbsentPathCache(MountTable mountTable, int numThreads) {
    mMountTable = mountTable;
    mCurrentPaths = new ConcurrentHashMap<>(8, 0.95f, 8);
    mCache = CacheBuilder.newBuilder().maximumSize(MAX_PATHS).recordStats().build();
    mThreads = numThreads;

    mPool = new ThreadPoolExecutor(mThreads, mThreads, THREAD_KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
        ThreadFactoryUtils.build("UFS-Absent-Path-Cache-%d", true));
    mPool.allowCoreThreadTimeOut(true);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_ABSENT_CACHE_SIZE.getName(),
        mCache::size);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_ABSENT_CACHE_MISSES.getName(),
        () -> mCache.stats().missCount());
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_ABSENT_CACHE_HITS.getName(),
        () -> mCache.stats().hitCount());
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_UFS_ABSENT_PATH_CACHE_SIZE.getName(), mCache::size, 2, TimeUnit.SECONDS);
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_UFS_ABSENT_PATH_CACHE_QUEUE_SIZE.getName(),
        () -> mPool.getQueue().size(), 2, TimeUnit.SECONDS);
  }

  @Override
  public void processAsync(AlluxioURI path, List<Inode> prefixInodes) {
    mPool.submit(() -> processPathSync(path, prefixInodes));
  }

  @Override
  public void addSinglePath(AlluxioURI path) {
    MountInfo mountInfo = getMountInfo(path);
    if (mountInfo == null) {
      return;
    }
    addCacheEntry(path.getPath(), mountInfo);
  }

  @Override
  public void processExisting(AlluxioURI path) {
    MountInfo mountInfo = getMountInfo(path);
    if (mountInfo == null) {
      return;
    }
    // This is called when we create a persisted path in Alluxio. The path components need to be
    // invalidated so the cache does not incorrectly think a path is absent.
    // As an optimization, this method avoids holding locks, to prevent waiting on UFS. However,
    // since the locks are not being used in this code path, there could be a race between this
    // invalidating thread, and a processing of the path from the thread pool. To avoid the race,
    // this invalidating thread must set the intention to invalidate before invalidating.
    for (AlluxioURI alluxioUri : getNestedPaths(path, mountInfo.getAlluxioUri().getDepth())) {
      PathLock pathLock = mCurrentPaths.get(alluxioUri.getPath());
      if (pathLock != null) {
        pathLock.setInvalidate();
      }
      removeCacheEntry(alluxioUri.getPath());
    }
  }

  @Override
  public boolean isAbsentSince(AlluxioURI path, long absentSince) {
    MountInfo mountInfo = getMountInfo(path);
    if (mountInfo == null) {
      return false;
    }
    AlluxioURI mountBaseUri = mountInfo.getAlluxioUri();

    while (path != null && !path.equals(mountBaseUri)) {
      Pair<Long, Long> cacheResult = mCache.getIfPresent(path.getPath());

      if (cacheResult != null && cacheResult.getFirst() != null
          && cacheResult.getSecond() != null
          && cacheResult.getFirst() >= absentSince
          && cacheResult.getSecond() == mountInfo.getMountId()) {
        return true;
      }
      path = path.getParent();
    }
    // Reached the root, without finding anything in the cache.
    return false;
  }

  /**
   * Processes and checks the existence of the corresponding ufs path for the given Alluxio path.
   *
   * @param alluxioUri the Alluxio path to process
   * @param mountInfo the associated {@link MountInfo} for the Alluxio path
   * @return if true, further traversal of the descendant paths should continue
   */
  private boolean processSinglePath(AlluxioURI alluxioUri, MountInfo mountInfo) {
    PathLock pathLock = new PathLock();
    Lock writeLock = pathLock.writeLock();
    Lock readLock = null;
    try {
      // Write lock this path, to only enable a single task per path
      writeLock.lock();
      PathLock existingLock = mCurrentPaths.putIfAbsent(alluxioUri.getPath(), pathLock);
      if (existingLock != null) {
        // Another thread already locked this path and is processing it. Wait for the other
        // thread to finish, by locking the existing read lock.
        writeLock.unlock();
        writeLock = null;
        readLock = existingLock.readLock();
        readLock.lock();

        if (mCache.getIfPresent(alluxioUri.getPath()) != null) {
          // This path is already in the cache (is absent). Further traversal is unnecessary.
          return false;
        }
      } else {
        // This thread has the exclusive lock for this path.

        // Resolve this Alluxio uri. It should match the original mount id.
        MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
        if (resolution.getMountId() != mountInfo.getMountId()) {
          // This mount point has changed. Further traversal is unnecessary.
          return false;
        }

        boolean existsInUfs;
        try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          existsInUfs = ufs.exists(resolution.getUri().toString());
        }
        if (existsInUfs) {
          // This ufs path exists. Remove the cache entry.
          removeCacheEntry(alluxioUri.getPath());
        } else {
          // This is the first ufs path which does not exist. Add it to the cache.
          addCacheEntry(alluxioUri.getPath(), mountInfo);

          if (pathLock.isInvalidate()) {
            // This path was marked to be invalidated, meaning this UFS path was just created,
            // and now exists. Invalidate the entry.
            // This check is necessary to avoid the race with the invalidating thread.
            removeCacheEntry(alluxioUri.getPath());
          } else {
            // Further traversal is unnecessary.
            return false;
          }
        }
      }
    } catch (InvalidPathException | IOException e) {
      LOG.warn("Processing path failed: " + alluxioUri, e);
      return false;
    } finally {
      // Unlock the path
      if (readLock != null) {
        readLock.unlock();
      }
      if (writeLock != null) {
        mCurrentPaths.remove(alluxioUri.getPath(), pathLock);
        writeLock.unlock();
      }
    }
    return true;
  }

  /**
   * @param alluxioUri the Alluxio path to get the mount info for
   * @return the {@link MountInfo} of the given Alluxio path, or null if it doesn't exist
   */
  private MountInfo getMountInfo(AlluxioURI alluxioUri) {
    try {
      MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
      return mMountTable.getMountInfo(resolution.getMountId());
    } catch (Exception e) {
      // Catch Exception in case the mount point doesn't exist currently.
      LOG.warn("Failed to get mount info for path {}. message: {}", alluxioUri, e.toString());
      return null;
    }
  }

  /**
   * Returns a sequence of Alluxio paths for a specified path, starting from the path component at
   * a specific index, to the specified path.
   *
   * @param alluxioUri the Alluxio path to get the nested paths for
   * @param startComponentIndex the index to the starting path component,
   *        root directory has index 0
   * @return a list of nested paths from the starting component to the given path
   */
  private List<AlluxioURI> getNestedPaths(AlluxioURI alluxioUri, int startComponentIndex) {
    try {
      String[] fullComponents = PathUtils.getPathComponents(alluxioUri.getPath());
      String[] baseComponents = Arrays.copyOfRange(fullComponents, 0, startComponentIndex);
      AlluxioURI uri = new AlluxioURI(
          PathUtils.concatPath(AlluxioURI.SEPARATOR, baseComponents));
      List<AlluxioURI> components = new ArrayList<>(fullComponents.length - startComponentIndex);
      for (int i = startComponentIndex; i < fullComponents.length; i++) {
        uri = uri.joinUnsafe(fullComponents[i]);
        components.add(uri);
      }
      return components;
    } catch (InvalidPathException e) {
      return Collections.emptyList();
    }
  }

  /**
   * This represents a lock for a path component.
   */
  private final class PathLock {
    private final ReadWriteLock mRwLock;
    private volatile boolean mInvalidate;

    private PathLock() {
      mRwLock = new ReentrantReadWriteLock();
      mInvalidate = false;
    }

    /**
     * @return the write lock
     */
    private Lock writeLock() {
      return mRwLock.writeLock();
    }

    /**
     * @return the read lock
     */
    private Lock readLock() {
      return mRwLock.readLock();
    }

    /**
     * Sets the intention to invalidate this path.
     */
    private void setInvalidate() {
      mInvalidate = true;
    }

    /**
     * @return true if the path was marked to be invalidated
     */
    private boolean isInvalidate() {
      return mInvalidate;
    }
  }

  private void addCacheEntry(String path, MountInfo mountInfo) {
    LOG.debug("Add cacheEntry={}", path);
    mCache.put(path, new Pair<Long, Long>(System.currentTimeMillis(), mountInfo.getMountId()));
  }

  private void removeCacheEntry(String path) {
    LOG.debug("Remove cacheEntry={}", path);
    mCache.invalidate(path);
  }

  /**
   * Processes a path synchronously.
   *
   * @param path the path to add
   * @param prefixInodes the existing inodes for the path prefix
   */
  @VisibleForTesting
  void processPathSync(AlluxioURI path, List<Inode> prefixInodes) {
    MountInfo mountInfo = getMountInfo(path);
    if (mountInfo == null) {
      return;
    }

    // baseIndex should be the index of the first non-persisted inode under the mount point.
    int baseIndex = mountInfo.getAlluxioUri().getDepth();
    while (baseIndex < prefixInodes.size()) {
      if (prefixInodes.get(baseIndex).isPersisted()) {
        baseIndex++;
      } else {
        break;
      }
    }

    for (AlluxioURI alluxioUri : getNestedPaths(path, baseIndex)) {
      if (!processSinglePath(alluxioUri, mountInfo)) {
        break;
      }
    }
  }
}
