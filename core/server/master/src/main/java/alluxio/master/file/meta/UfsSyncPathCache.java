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

import alluxio.Configuration;
import alluxio.PropertyKey;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final Cache<String, Long> mCache;

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
   */
  public void notifySyncedPath(String path) {
    mCache.put(path, System.currentTimeMillis());
  }

  /**
   * @param path the path to check
   * @param intervalMs the sync interval, in ms
   * @return true if a sync should occur for the path and interval setting, false otherwise
   */
  public boolean shouldSyncPath(String path, long intervalMs) {
    if (intervalMs < 0) {
      // Never sync.
      return false;
    }
    if (intervalMs == 0) {
      // Always sync.
      return true;
    }
    Long lastSync = mCache.getIfPresent(path);
    if (lastSync == null) {
      // No info about the last sync, so trigger a sync.
      return true;
    }
    return (System.currentTimeMillis() - lastSync) >= intervalMs;
  }
}
