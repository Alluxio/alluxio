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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.RpcContext;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.resource.CloseableResource;
import alluxio.util.LogUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a cache from an Alluxio namespace URI ({@link AlluxioURI}, i.e. /path/to/inode) to
 * UFS statuses.
 *
 * It also allows associating a path with child inodes, so that the statuses for a specific path can
 * be searched for later.
 */
@ThreadSafe
public class UfsStatusCache {
  private static final Logger LOG = LoggerFactory.getLogger(UfsStatusCache.class);

  private final ConcurrentHashMap<AlluxioURI, UfsStatus> mStatuses;
  private final ConcurrentHashMap<AlluxioURI, Future<Collection<UfsStatus>>> mActivePrefetchJobs;
  private final ConcurrentHashMap<AlluxioURI, Collection<UfsStatus>> mChildren;
  private final UfsAbsentPathCache mAbsentCache;
  private final long mCacheValidTime;
  private final ExecutorService mPrefetchExecutor;

  /**
   * Create a new instance of {@link UfsStatusCache}.
   *
   * @param prefetchExecutor the executor service used to prefetch statuses. If set to null, then
   *                         calls to {@link #prefetchChildren(AlluxioURI, MountTable)} will not
   *                         schedule any tasks.
   * @param absentPathCache the absent cache that ufsStatusCache should consult
   * @param cacheValidTime  the time when the absent cache entry would be considered valid
   */
  public UfsStatusCache(@Nullable ExecutorService prefetchExecutor,
      UfsAbsentPathCache absentPathCache, long cacheValidTime) {
    mStatuses = new ConcurrentHashMap<>();
    mChildren = new ConcurrentHashMap<>();
    mActivePrefetchJobs = new ConcurrentHashMap<>();
    mAbsentCache = absentPathCache;
    mCacheValidTime = cacheValidTime;
    mPrefetchExecutor = prefetchExecutor;
  }

  /**
   * Add a new status to the cache.
   *
   * The last component of the path in the {@link AlluxioURI} must match the result of
   * {@link UfsStatus#getName()}. This method overrides any status currently cached for the same
   * URI.
   *
   * @param path the Alluxio path to key on
   * @param status the ufs status to store
   * @return the previous status for the path if it existed, null otherwise
   * @throws IllegalArgumentException if the status name doesn't match the final URI path component
   */
  @Nullable
  public UfsStatus addStatus(AlluxioURI path, UfsStatus status) {
    if (!path.getName().equals(status.getName())) {
      throw new IllegalArgumentException(
          String.format("path name %s does not match ufs status name %s",
              path.getName(), status.getName()));
    }
    mAbsentCache.processExisting(path);
    return mStatuses.put(path, status);
  }

  /**
   * Add a parent-child mapping to the status cache.
   *
   * All child statuses added via this method will be available via {@link #getStatus(AlluxioURI)}.
   *
   * @param path the directory inode path which contains the children
   * @param children the children of the {@code path}
   * @return the previous set of children if the mapping existed, null otherwise
   */
  @Nullable
  public Collection<UfsStatus> addChildren(AlluxioURI path, Collection<UfsStatus> children) {
    ConcurrentHashSet<UfsStatus> set = new ConcurrentHashSet<>();
    children.forEach(child -> {
      AlluxioURI childPath = path.joinUnsafe(child.getName());
      addStatus(childPath, child);
      set.add(child);
    });
    return mChildren.put(path, set);
  }

  /**
   * Remove a status from the cache.
   *
   * This will remove any references to child {@link UfsStatus}.
   *
   * @param path the path corresponding to the {@link UfsStatus} to remove
   * @return the removed UfsStatus
   */
  @Nullable
  public UfsStatus remove(AlluxioURI path) {
    Preconditions.checkNotNull(path, "can't remove null status cache path");
    UfsStatus removed = mStatuses.remove(path);
    mChildren.remove(path); // ok if there aren't any children
    return removed;
  }

  private void checkAbsentCache(AlluxioURI path) throws FileNotFoundException {
    if (mAbsentCache.isAbsentSince(path, mCacheValidTime)) {
      throw new FileNotFoundException("UFS Status not found for path " + path.toString());
    }
  }

  /**
   * Get the UfsStatus from a given AlluxioURI.
   *
   * @param path the path the retrieve
   * @return The corresponding {@link UfsStatus} or {@code null} if there is none stored
   * @throws FileNotFoundException if the UFS does not contain the file
   */
  @Nullable
  public UfsStatus getStatus(AlluxioURI path) throws FileNotFoundException {
    checkAbsentCache(path);
    return mStatuses.get(path);
  }

  /**
   * Attempts to return a status from the cache. If it doesn't exist, reaches to the UFS for it.
   *
   * @param path the path the retrieve
   * @param mountTable the Alluxio mount table
   * @return The corresponding {@link UfsStatus} or {@code null} if there is none stored
   */
  @Nullable
  public UfsStatus fetchStatusIfAbsent(AlluxioURI path, MountTable mountTable)
      throws InvalidPathException, IOException {
    UfsStatus status;
    try {
      status = getStatus(path);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (status != null) {
      return status;
    }
    MountTable.Resolution resolution = mountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      UfsStatus ufsStatus = ufs.getStatus(ufsUri.toString());
      if (ufsStatus == null) {
        mAbsentCache.addSinglePath(path);
        return null;
      }
      ufsStatus.setName(path.getName());
      addStatus(path, ufsStatus);
      return ufsStatus;
    } catch (FileNotFoundException e) {
      // If the ufs can not find the file, we explicitly mark it absent so we do not recheck it
      mAbsentCache.addSinglePath(path);
    } catch (IllegalArgumentException | IOException e) {
      LogUtils.warnWithException(LOG, "Failed to fetch status for {}", path, e);
    }
    return null;
  }

  /**
   * Fetches children of a given alluxio path, stores them in the cache, then returns them.
   *
   * Children can be returned in a few ways
   * 1. Children already exist in the internal index. We simply return them
   * 2. If children did not already exist in the index, then check if there was a scheduled
   * prefetch job running for this path. If so, wait for the job to finish and return the result.
   * 3. If no prefetch job, and children don't yet exist in the cache, then if the fallback
   * parameter is true, fetch them from the UFS and store them in the cache. Otherwise, simply
   * return null.
   *
   * @param rpcContext the rpcContext of the source of this call
   * @param path the Alluxio path to get the children of
   * @param mountTable the Alluxio mount table
   * @param useFallback whether or not to fall back to calling the UFS
   * @return child UFS statuses of the alluxio path, or null if no prefetch job and fallback
   *         specified as false
   * @throws InvalidPathException if the alluxio path can't be resolved to a UFS mount
   */
  @Nullable
  public Collection<UfsStatus> fetchChildrenIfAbsent(RpcContext rpcContext, AlluxioURI path,
                                                     MountTable mountTable, boolean useFallback)
      throws InterruptedException, InvalidPathException {
    Future<Collection<UfsStatus>> prefetchJob = mActivePrefetchJobs.get(path);
    if (prefetchJob != null) {
      while (true) {
        try {
          return prefetchJob.get(100, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          if (rpcContext != null) {
            rpcContext.throwIfCancelled();
          }
        } catch (InterruptedException | ExecutionException e) {
          LogUtils.warnWithException(LOG,
              "Failed to get result for prefetch job on alluxio path {}", path, e);
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw (InterruptedException) e;
          }
          break;
        } finally {
          mActivePrefetchJobs.remove(path);
        }
      }
    }
    Collection<UfsStatus> children = getChildren(path);
    if (children != null) {
      return children;
    }

    if (useFallback) {
      return getChildrenIfAbsent(path, mountTable);
    }
    return null;
  }

  /**
   * Fetches children of a given alluxio path stores them in the cache, then returns them.
   *
   * Will always return statuses from the UFS whether or not they exist in the cache, and whether
   * a prefetch job was scheduled or not.
   *
   * @param rpcContext the rpcContext of the source of this call
   * @param path the Alluxio path
   * @param mountTable the Alluxio mount table
   * @return child UFS statuses of the alluxio path
   * @throws InvalidPathException if the alluxio path can't be resolved to a UFS mount
   */
  @Nullable
  public Collection<UfsStatus> fetchChildrenIfAbsent(RpcContext rpcContext, AlluxioURI path,
                                                     MountTable mountTable)
      throws InterruptedException, InvalidPathException {
    return fetchChildrenIfAbsent(rpcContext, path, mountTable, true);
  }

  /**
   * Retrieves the child UFS statuses for a given path and stores them in the cache.
   *
   * This method first checks if the children have already been retrieved, and if not, then
   * retrieves them.

   * @param path the path to get the children for
   * @param mountTable the Alluxio mount table
   * @return the child statuses that were stored in the cache, or null if the UFS couldn't list the
   *         statuses
   * @throws InvalidPathException when the table can't resolve the mount for the given URI
   */
  @Nullable
  Collection<UfsStatus> getChildrenIfAbsent(AlluxioURI path, MountTable mountTable)
      throws InvalidPathException {
    Collection<UfsStatus> children = getChildren(path);
    if (children != null) {
      return children;
    }
    if (mAbsentCache.isAbsentSince(path, mCacheValidTime)) {
      return null;
    }
    MountTable.Resolution resolution = mountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      UfsStatus[] statuses = ufs.listStatus(ufsUri.toString());
      if (statuses == null) {
        mAbsentCache.addSinglePath(path);
        return null;
      }
      children = Arrays.asList(statuses);
      addChildren(path, children);
    } catch (IllegalArgumentException | IOException e) {
      LOG.debug("Failed to add status to cache {}", path, e);
    }
    return children;
  }

  /**
   * Get the child {@link UfsStatus}es from a given {@link AlluxioURI}.
   *
   * @param path the path the retrieve
   * @return The corresponding {@link UfsStatus} or {@code null} if there is none stored
   */
  @Nullable
  public Collection<UfsStatus> getChildren(AlluxioURI path) {
    return mChildren.get(path);
  }

  /**
   * Submit a request to asynchronously fetch the statuses corresponding to a given directory.
   *
   * Retrieve any fetched statuses by calling
   * {@link #fetchChildrenIfAbsent(RpcContext, AlluxioURI, MountTable)} with the same Alluxio path.
   *
   * If no {@link ExecutorService} was provided to this object before instantiation, this method is
   * a no-op.
   *
   * @param path the path to prefetch
   * @param mountTable the Alluxio mount table
   * @return the future corresponding to the fetch task
   */
  @Nullable
  public Future<Collection<UfsStatus>> prefetchChildren(AlluxioURI path, MountTable mountTable) {
    if (mPrefetchExecutor == null) {
      return null;
    }
    try {
      Future<Collection<UfsStatus>> job =
          mPrefetchExecutor.submit(() -> getChildrenIfAbsent(path, mountTable));
      Future<Collection<UfsStatus>> prev = mActivePrefetchJobs.put(path, job);
      if (prev != null) {
        prev.cancel(true);
      }
      return job;
    } catch (RejectedExecutionException e) {
      LOG.debug("Failed to submit prefetch job for path {}", path, e);
      return null;
    }
  }

  /**
   * Interrupts and cancels any currently running prefetch jobs.
   */
  public void cancelAllPrefetch() {
    for (Future<?> f : mActivePrefetchJobs.values()) {
      f.cancel(false);
    }
    mActivePrefetchJobs.clear();
  }
}
