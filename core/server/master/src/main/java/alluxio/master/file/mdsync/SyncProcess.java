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

package alluxio.master.file.mdsync;

import alluxio.master.file.meta.UfsSyncPathCache;

/**
 * The sync process interfaces.
 */
public interface SyncProcess {
  /**
   * Performs a sync.
   * @param loadResult the UFS load result
   * @param syncPathCache the sync path cache for updating the last sync time
   * @return the sync process result
   */
  SyncProcessResult performSync(
      LoadResult loadResult, UfsSyncPathCache syncPathCache) throws Throwable;
}

