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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.util.io.PathUtils;

import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
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
      Configuration.getInt(PropertyKey.MASTER_UFS_PATH_CACHE_CAPACITY);

  /** Cache of paths which have been synced. */
  public final Cache<String, SyncTime> mCache;

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
    SyncTime lastSync = mCache.getIfPresent(path);
    if (lastSync == null) {
      LOG.trace("Sync record not found, trigger sync");
      return true;
    }

    if (intervalMs < 0) {
      // Never sync.
      LOG.trace("{} path specified interval<0, skip sync", path);
      return false;
    }
    if (intervalMs == 0) {
      // Always sync.
      LOG.trace("{} path specified interval=0, force sync", path);
      return true;
    }

    // check the last sync information for the path itself.
    if (!shouldSyncInternal(lastSync, intervalMs, false)) {
      LOG.trace("{} path should sync based on last sync TS", path);
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
          LOG.trace("{} path sync skipped because an ancestor is recursively synced", path);
          return false;
        }
      } catch (InvalidPathException e) {
        // this is not expected, but the sync should be triggered just in case.
        LOG.debug("Failed to get parent of ({}), for checking sync for ({})", currPath, path);
        return true;
      }
    }

    // trigger a sync, because a sync on the path (or an ancestor) was performed recently
    // TODO(jiacheng): is this correct?
    return true;
  }

  /**
   * Invalidate cache.
   *
   * @param path
   */
  public void invalidateCache(String path) {
    // When there's no record of the last sync time, the file will be sync-ed
    mCache.invalidate(path);
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

  /**
   * Sync timestamp.
   */
  public static class SyncTime {
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

    /**
     *
     * @return the last recursive sync timestamp whose unit is ms
     */
    long getLastRecursiveSyncMs() {
      return mLastRecursiveSyncMs;
    }

    /**
     * Convert the timestamp to String.
     *
     * @param millis the timestamp with long type
     * @return the timestamp with String type
     */
    public static String toDateString(long millis) {
      if (millis == UNSYNCED) {
        return "UNSYNCED";
      }
      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss,SSS");
      Date date = new Date(millis);
      return sdf.format(date);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("LastSync", toDateString(mLastSyncMs))
              .add("LastRecursiveSync", toDateString(mLastRecursiveSyncMs))
              .toString();
    }
  }
}
