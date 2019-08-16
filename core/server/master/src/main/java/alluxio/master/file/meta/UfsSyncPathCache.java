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
   * @param path the path to check
   * @param intervalMs the sync interval, in ms
   * @param checkRecursive whether to check parent directory syncTime recursively
   * @return true if a sync should occur for the path and interval setting, false otherwise
   * @throws InvalidPathException
   */
  public boolean shouldSyncPath(String path, long intervalMs, boolean checkRecursive)
      throws InvalidPathException {
    if (intervalMs < 0) {
      // Never sync.
      return false;
    }
    if (intervalMs == 0) {
      // Always sync.
      return true;
    }

    if (checkRecursive) {
      return syncCheckRecursive(path, intervalMs);
    }
    SyncTime lastSync = mCache.getIfPresent(path);
    if (lastSync == null) {
      return true;
    }
    return (System.currentTimeMillis() - lastSync.getLastSyncMs()) >= intervalMs;
  }

  private boolean syncCheckRecursive(String path, long intervalMs) throws InvalidPathException {
    // sync should be done on this path, but check path itself and all ancestors to determine
    // if a recursive sync had been performed (to avoid a sync again).
    int parentLevel = 0;
    String currPath = path;
    SyncTimeLevel timeLevel = new SyncTimeLevel();
    adjustSyncTimeLevel(timeLevel, mCache.getIfPresent(currPath), parentLevel);
    while (!PathUtils.isRoot(currPath)) {
      currPath = PathUtils.getParent(currPath);
      adjustSyncTimeLevel(timeLevel, mCache.getIfPresent(currPath), ++parentLevel);
    }
    return shouldSyncInternal(timeLevel, intervalMs);
  }

  private void adjustSyncTimeLevel(SyncTimeLevel timeLevel, SyncTime lastSync, int parentLevel) {
    if (timeLevel == null || lastSync == null) {
      return;
    }
    if (timeLevel.getLastSyncMs() < lastSync.getLastSyncMs()) {
      timeLevel.setLastSyncMs(lastSync.getLastSyncMs(), parentLevel);
    }
    if (timeLevel.getLastRecursiveSyncMs() < lastSync.getLastRecursiveSyncMs()) {
      timeLevel.setLastRecursiveSyncMs(lastSync.getLastRecursiveSyncMs());
    }
    if (parentLevel <= 1 && timeLevel.getLastRecursiveSyncMs() < lastSync.getLastSyncMs()) {
      timeLevel.setLastRecursiveSyncMs(lastSync.getLastSyncMs());
    }
  }

  /**
   * Determines if the sync should be performed.
   *
   * @param timeLevel the {@link SyncTimeLevel} to examine
   * @param intervalMs the sync interval, in ms
   * @return true if the sync should be performed
   */
  private boolean shouldSyncInternal(@Nullable SyncTimeLevel timeLevel, long intervalMs) {
    long lastSyncMs = timeLevel.getLastSyncMs();
    if (timeLevel.getLastSyncPl() > 1) {
      lastSyncMs = timeLevel.getLastRecursiveSyncMs();
    }
    if (lastSyncMs == SyncTime.UNSYNCED) {
      // was not synced ever, so should sync
      return true;
    }
    return (System.currentTimeMillis() - lastSyncMs) >= intervalMs;
  }

  private static class SyncTimeLevel {
    /** the parent level for time mLastSyncMs. */
    private int mLastSyncPl;
    /** record the last time mLastSyncMs in SyncTime. */
    private long mLastSyncMs;
    /** record the last time mLastRecursiveSyncMs in SyncTime. */
    private long mLastRecursiveSyncMs;

    SyncTimeLevel() {
      mLastSyncPl = 0;
      mLastSyncMs = SyncTime.UNSYNCED;
      mLastRecursiveSyncMs = SyncTime.UNSYNCED;
    }

    void setLastSyncMs(long lastSyncMs, int lastSyncPl) {
      mLastSyncMs = lastSyncMs;
      mLastSyncPl = lastSyncPl;
    }

    void setLastRecursiveSyncMs(long lastRecursiveSyncMs) {
      mLastRecursiveSyncMs = lastRecursiveSyncMs;
    }

    int getLastSyncPl() {
      return mLastSyncPl;
    }

    long getLastSyncMs() {
      return mLastSyncMs;
    }

    long getLastRecursiveSyncMs() {
      return mLastRecursiveSyncMs;
    }
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
