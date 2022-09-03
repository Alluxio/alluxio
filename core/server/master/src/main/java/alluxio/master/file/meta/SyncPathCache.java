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
import alluxio.file.options.DescendantType;

import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;

/**
 * Interface for sync.
 */
public interface SyncPathCache {
  /**
   * Notify sync happened.
   * @param path the synced path
   * @param descendantType the descendant type for the performed operation
   * @param startTime the value returned from startSync
   *                  TODO(tcrain) note that this value
   *                  is currently unused, but will be used in an upcoming PR for
   *                  cross cluster sync)
   * @param syncTime the time to set the sync success to, if null then the current
   *                 clock time is used
   */
  void notifySyncedPath(AlluxioURI path, DescendantType descendantType, long startTime,
                        Long syncTime);

  /**
   * Check if sync should happen.
   * @param path the path to check
   * @param intervalMs the frequency in ms that the sync should happen
   * @param descendantType the descendant type of the opeation being performed
   * @return true if should sync
   */
  SyncCheck shouldSyncPath(AlluxioURI path, long intervalMs, DescendantType descendantType);

  /**
   * Called when starting a sync.
   * @param path the path being synced
   * @return the time at the start of the sync
   */
  long startSync(AlluxioURI path);

  /**
   * Get sync times for a given path if they exist in the cache.
   * @param path the path to check
   * @return a pair of sync times, where element 0 is the direct sync time
   * and element 1 is the recursive sync time
   */
  @VisibleForTesting
  Optional<Pair<Long, Long>> getSyncTimesForPath(AlluxioURI path);
}
