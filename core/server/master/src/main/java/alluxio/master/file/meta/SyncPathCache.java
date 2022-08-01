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
   * notify sync happened.
   * @param path
   * @param descendantType
   */
  void notifySyncedPath(AlluxioURI path, DescendantType descendantType);

  /**
   * Called instead of notifySyncedPath in case of failure.
   * @param path
   */
  void failedSyncPath(AlluxioURI path);

  /**
   * check if sync should happen.
   * @param path
   * @param intervalMs
   * @param descendantType
   * @return true if should sync
   */
  boolean shouldSyncPath(AlluxioURI path, long intervalMs, DescendantType descendantType);

  /**
   * called when starting a sync.
   * @param path
   */
  void startSync(AlluxioURI path);
}
