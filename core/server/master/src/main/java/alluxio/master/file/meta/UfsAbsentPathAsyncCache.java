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
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.InvalidPathException;
import alluxio.util.ThreadFactoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This is a version of {@link UfsAbsentPathCache} which implements an asynchronous add to the
 * cache, since the processing of the path may be slow.
 */
@ThreadSafe
public final class UfsAbsentPathAsyncCache extends UfsAbsentPathCache {
  private static final Logger LOG = LoggerFactory.getLogger(UfsAbsentPathAsyncCache.class);
  /** Number of threads for the async pool.  */
  private static final int UFS_ABSENT_PATH_CACHE_THREADS = 50;

  /** A set of paths currently being processed. This is used to prevent duplicate processing.  */
  private final ConcurrentHashSet<String> mCurrentPaths;

  /** A thread pool for the async tasks. */
  private final ThreadPoolExecutor mPool;

  /**
   * Creates a new instance of {@link UfsAbsentPathAsyncCache}.
   *
   * @param mountTable the mount table
   */
  public UfsAbsentPathAsyncCache(MountTable mountTable) {
    super(mountTable);
    mCurrentPaths = new ConcurrentHashSet<>(8, 0.95f, 8);

    mPool = new ThreadPoolExecutor(UFS_ABSENT_PATH_CACHE_THREADS, UFS_ABSENT_PATH_CACHE_THREADS, 1,
        TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
        ThreadFactoryUtils.build("UFS-Absent-Path-Cache-%d", true));
  }

  @Override
  public void addAbsentPath(AlluxioURI path) {
    if (mCurrentPaths.contains(path.toString())) {
      // already being processed by another thread.
      return;
    }
    mPool.submit(new ProcessPathTask(path));
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
      if (!mCurrentPaths.addIfAbsent(mPath.toString())) {
        // already being processed by another thread.
        return;
      }
      try {
        if (isAbsent(mPath)) {
          // this path his already considered absent, so no need to add it again.
          return;
        }
        UfsAbsentPathAsyncCache.super.addAbsentPath(mPath);
      } catch (InvalidPathException e) {
        // Ignore the exception
      } finally {
        mCurrentPaths.remove(mPath.toString());
      }
    }
  }
}
