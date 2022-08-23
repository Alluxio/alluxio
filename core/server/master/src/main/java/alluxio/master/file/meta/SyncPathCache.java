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
import alluxio.file.options.DescendantType;

/**
 * Interface for sync.
 */
public interface SyncPathCache {
  /**
   * Notify sync happened.
   * @param path the synced path
   * @param descendantType the descendant type for the performed operation
   * @param startTime the value returned from startSync
   * @param syncTime the time to set the sync success to, if null then the current
   *                 clock time is used
   */
  void notifySyncedPath(AlluxioURI path, DescendantType descendantType, long startTime,
                        Long syncTime);

  /**
   * Called instead of notifySyncedPath in case of failure.
   * @param path the path of the failed sync
   */
  void failedSyncPath(AlluxioURI path);

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
   * @return true if this cache is for a cross cluster mount, false otherwise
   */
  boolean isCrossCluster();
}
