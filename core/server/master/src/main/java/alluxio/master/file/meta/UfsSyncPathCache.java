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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.util.io.PathUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This cache maintains the Alluxio paths which have been synced with UFS.
 */
@ThreadSafe
public final class UfsSyncPathCache {
  private static final Logger LOG = LoggerFactory.getLogger(UfsSyncPathCache.class);

  /** Number of paths to cache. */
  private static final int MAX_PATHS =
      ServerConfiguration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY);

  /** Cache of paths which have been synced. */
  private final Cache<String, SyncTime> mCache;

  /**
   * Creates a new instance of {@link UfsSyncPathCache}.
   */
  public UfsSyncPathCache() {
    mCache = CacheBuilder.newBuilder().maximumSize(MAX_PATHS).build();
  }

  /**
   * Notifies the cache that the path was synced.
   *
   * @param path the path that was synced
   * @param descendantType the descendant type that the path was synced with
   */
  public void notifySyncedPath(String path, DescendantType descendantType) {
    long syncTimeMs = System.currentTimeMillis();
    mCache.asMap().compute(path, (key, oldSyncTime) -> {
      if (oldSyncTime != null) {
        // update the existing sync time
        oldSyncTime.updateSync(syncTimeMs, descendantType);
        return oldSyncTime;
      }
      return new SyncTime(syncTimeMs, descendantType);
    });
  }

  /**
   * The logic of shouldSyncPath need to consider the difference between file and directory,
   * with the variable isGetFileInfo we just process getFileInfo specially.
   *
   * There are three cases needed to address:
   * 1. the ancestor directories
   * 2. the direct parent directory
   * 3. the difference with file and directory
   *
   * @param path the path to check
   * @param intervalMs the sync interval, in ms
   * @param isGetFileInfo the operate is from getFileInfo or not
   * @return true if a sync should occur for the path and interval setting, false otherwise
   */
  public boolean shouldSyncPath(String path, long intervalMs, boolean isGetFileInfo) {
    if (intervalMs < 0) {
      // Never sync.
      return false;
    }
    if (intervalMs == 0) {
      // Always sync.
      return true;
    }

    // check the last sync information for the path itself.
    SyncTime lastSync = mCache.getIfPresent(path);
    if (!shouldSyncInternal(lastSync, intervalMs, false)) {
      // Sync is not necessary for this path.
      return false;
    }

    // sync should be done on this path, but check all ancestors to determine if a recursive sync
    // had been performed (to avoid a sync again).
    int parentLevel = 0;
    String currPath = path;
    while (!currPath.equals(AlluxioURI.SEPARATOR)) {
      try {
        currPath = PathUtils.getParent(currPath);
        parentLevel++;
        lastSync = mCache.getIfPresent(currPath);
        if (!shouldSyncInternal(lastSync, intervalMs, parentLevel > 1 || !isGetFileInfo)) {
          // Sync is not necessary because an ancestor was already recursively synced
          return false;
        }
      } catch (InvalidPathException e) {
        // this is not expected, but the sync should be triggered just in case.
        LOG.debug("Failed to get parent of ({}), for checking sync for ({})", currPath, path);
        return true;
      }
    }

    // trigger a sync, because a sync on the path (or an ancestor) was performed recently
    return true;
  }

  /**
   * Determines if the sync should be performed.
   *
   * @param syncTime the {@link SyncTime} to examine
   * @param intervalMs the sync interval, in ms
   * @param checkRecursive checks the recursive sync time if true, checks the standard sync time
   *                       otherwise
   * @return true if the sync should be performed
   */
  private boolean shouldSyncInternal(@Nullable SyncTime syncTime, long intervalMs,
      boolean checkRecursive) {
    if (syncTime == null) {
      return true;
    }
    long lastSyncMs = syncTime.getLastSyncMs();
    if (checkRecursive) {
      lastSyncMs = syncTime.getLastRecursiveSyncMs();
    }
    if (lastSyncMs == SyncTime.UNSYNCED) {
      // was not synced ever, so should sync
      return true;
    }
    return (System.currentTimeMillis() - lastSyncMs) >= intervalMs;
  }

  private static class SyncTime {
    static final long UNSYNCED = -1;
    /** the last time (in ms) that a sync was performed. */
    private long mLastSyncMs;
    /** the last time (in ms) that a recursive sync was performed. */
    private long mLastRecursiveSyncMs;

    SyncTime(long syncMs, DescendantType descendantType) {
      mLastSyncMs = UNSYNCED;
      mLastRecursiveSyncMs = UNSYNCED;
      updateSync(syncMs, descendantType);
    }

    void updateSync(long syncMs, DescendantType descendantType) {
      mLastSyncMs = syncMs;
      if (descendantType == DescendantType.ALL) {
        mLastRecursiveSyncMs = syncMs;
      }
    }

    long getLastSyncMs() {
      return mLastSyncMs;
    }

    long getLastRecursiveSyncMs() {
      return mLastRecursiveSyncMs;
    }
  }
}
