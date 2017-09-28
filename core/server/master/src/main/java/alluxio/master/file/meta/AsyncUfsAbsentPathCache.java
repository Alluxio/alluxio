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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
      Configuration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY);

  /** The mount table. */
  private final MountTable mMountTable;
  /** Paths currently being processed. This is used to prevent duplicate processing. */
  private final ConcurrentHashMapV8<String, PathLock> mCurrentPaths;
  /** Cache of paths which are absent in the ufs. */
  private final Cache<String, Long> mCache;
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
    mCurrentPaths = new ConcurrentHashMapV8<>(8, 0.95f, 8);
    mCache = CacheBuilder.newBuilder().maximumSize(MAX_PATHS).build();
    mThreads = numThreads;

    mPool = new ThreadPoolExecutor(mThreads, mThreads, THREAD_KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
        ThreadFactoryUtils.build("UFS-Absent-Path-Cache-%d", true));
  }

  @Override
  public void process(AlluxioURI path) {
    mPool.submit(new ProcessPathTask(path));
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
    for (AlluxioURI alluxioUri : getNestedPaths(path, mountInfo)) {
      PathLock pathLock = mCurrentPaths.get(alluxioUri.getPath());
      if (pathLock != null) {
        pathLock.setInvalidate();
      }
      mCache.invalidate(alluxioUri.getPath());
    }
  }

  @Override
  public boolean isAbsent(AlluxioURI path) {
    MountInfo mountInfo = getMountInfo(path);
    if (mountInfo == null) {
      return false;
    }
    AlluxioURI mountBaseUri = mountInfo.getAlluxioUri();

    while (path != null && !path.equals(mountBaseUri)) {
      Long cached = mCache.getIfPresent(path.getPath());
      if (cached != null && cached == mountInfo.getMountId()) {
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

        UnderFileSystem ufs = resolution.getUfs();
        if (ufs.exists(resolution.getUri().toString())) {
          // This ufs path exists. Remove the cache entry.
          mCache.invalidate(alluxioUri.getPath());
        } else {
          // This is the first ufs path which does not exist. Add it to the cache.
          mCache.put(alluxioUri.getPath(), mountInfo.getMountId());

          if (pathLock.isInvalidate()) {
            // This path was marked to be invalidated, meaning this UFS path was just created,
            // and now exists. Invalidate the entry.
            // This check is necessary to avoid the race with the invalidating thread.
            mCache.invalidate(alluxioUri.getPath());
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
      LOG.warn("Failed to get mount info for path {}. message: {}", alluxioUri, e.getMessage());
      return null;
    }
  }

  /**
   * Returns a sequence of Alluxio paths for a specified path, starting from the base of the
   * mount point, to the specified path.
   *
   * @param alluxioUri the Alluxio path to get the nested paths for
   * @param mountInfo the mount info for this Alluxio path
   * @return a list of nested paths from the mount base to the given path
   */
  private List<AlluxioURI> getNestedPaths(AlluxioURI alluxioUri, MountInfo mountInfo) {
    try {
      String[] fullComponents = PathUtils.getPathComponents(alluxioUri.getPath());
      // create a uri of the base of the mount point.
      AlluxioURI mountBaseUri = mountInfo.getAlluxioUri();
      int mountBaseDepth = mountBaseUri.getDepth();

      List<AlluxioURI> components = new ArrayList<>(fullComponents.length - mountBaseDepth);
      for (int i = 0; i < fullComponents.length; i++) {
        if (i > 0 && i <= mountBaseDepth) {
          // Do not include components before the base of the mount point.
          // However, include the first component, since that will be the actual mount point base.
          continue;
        }
        mountBaseUri = mountBaseUri.join(fullComponents[i]);
        components.add(mountBaseUri);
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

  /**
   * This is the async task for adding a path to the cache.
   */
  private final class ProcessPathTask implements Runnable {
    private final AlluxioURI mPath;

    /**
     * @param path the path to add
     */
    private ProcessPathTask(AlluxioURI path) {
      mPath = path;
    }

    @Override
    public void run() {
      MountInfo mountInfo = getMountInfo(mPath);
      if (mountInfo == null) {
        return;
      }
      for (AlluxioURI alluxioUri : getNestedPaths(mPath, mountInfo)) {
        if (!processSinglePath(alluxioUri, mountInfo)) {
          break;
        }
      }
    }
  }
}
