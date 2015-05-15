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

import tachyon.worker.tiered.StorageDir;

/**
 * Space allocation strategy from dirs in the same StorageTier.
 */
public interface AllocateStrategy {
  /**
   * Check whether it is possible to get enough space by evicting some blocks
   *
   * @param storageDirs A list of candidate StorageDir to allocate space
   * @param requestSizeBytes The size of requested space in bytes
   * @return true if possible, false otherwise
   */
  boolean fitInPossible(StorageDir[] storageDirs, long requestSizeBytes);

  /**
   * Allocate space on StorageDirs. It returns the affordable StorageDir according to
   * AllocateStrategy, null if no enough space available on StorageDirs.
   *
   * @param storageDirs A list of candidate StorageDir to allocate space
   * @param userId The id of user
   * @param requestSizeBytes The size of requested space in bytes
   * @return StorageDir assigned, null if no StorageDir is found
   */
  StorageDir getStorageDir(StorageDir[] storageDirs, long userId, long requestSizeBytes);
}
