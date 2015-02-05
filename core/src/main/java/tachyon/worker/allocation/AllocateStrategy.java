/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.allocation;

import tachyon.worker.hierarchy.StorageDir;

/**
 * Space allocation strategy from dirs in the same StorageTier.
 */
public interface AllocateStrategy {
  /**
   * Check whether it is possible to get enough space by evicting some blocks
   *
   * @param storageDirs candidates of StorageDirs which space will be allocated in
   * @param requestSizeBytes requested space in bytes
   * @return true if possible, false otherwise
   */
  public boolean fitInPossible(StorageDir[] storageDirs, long requestSizeBytes);

  /**
   * Allocate space on StorageDirs. It returns the affordable StorageDir according to
   * AllocateStrategy, null if no enough space available on StorageDirs.
   *
   * @param storageDirs candidates of StorageDirs that space will be allocated in
   * @param userId id of user
   * @param requestSizeBytes size to request in bytes
   * @return StorageDir assigned, null if no StorageDir is assigned
   */
  public StorageDir getStorageDir(StorageDir[] storageDirs, long userId, long requestSizeBytes);
}
