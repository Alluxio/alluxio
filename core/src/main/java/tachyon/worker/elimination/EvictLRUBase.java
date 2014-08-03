/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.elimination;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import tachyon.Pair;
import tachyon.master.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

public abstract class EvictLRUBase implements EvictStrategy {

  StorageDir[] mStorageDirs;
  
  EvictLRUBase(StorageDir[] storageDirs) {
    mStorageDirs = storageDirs;
  }

  /**
   * if current block can be evicted
   * 
   * @param blockId
   *          id of the block
   * @param pinList
   *          list of pinned files
   * @param isLastTier
   *          whether current storage tier is the last tier
   * @return true if can be evicted, false otherwise
   */
  boolean blockEvictable(long blockId, Set<Integer> pinList, boolean isLastTier) {
    if (isLastTier && pinList.contains(BlockInfo.computeInodeId(blockId))) {
      return false;
    }
    return true;
  }

  /**
   * get oldest block access information
   * 
   * @param curDir
   *          current storage dir
   * @param toEvictBlockIds
   *          block ids that already selected to be evicted
   * @param pinList
   *          list of pinned files
   * @param isLastTier
   *          whether current storage tier is the last tier
   * @return oldest access information of current storage dir
   */
  Pair<Long, Long> getLRUBlock(StorageDir curDir, Collection<Long> toEvictBlockIds,
      Set<Integer> pinList, boolean isLastTier) {
    long blockId = -1;
    long oldestTime = Long.MAX_VALUE;
    Map<Long, Long> accessTimes = curDir.getLastBlockAccessTime();
    Map<Long, Set<Long>> lockedBlocks = curDir.getUsersPerLockedBlock();
    // synchronized (accessTimes) { not used in following block 
    // synchronized (lockedBlocks) {
    for (Entry<Long, Long> accessTime : accessTimes.entrySet()) {
      if (toEvictBlockIds.contains(accessTime.getKey())) {
        continue;
      }
      if (accessTime.getValue() < oldestTime && !lockedBlocks.containsKey(accessTime.getKey())) {
        if (blockEvictable(accessTime.getKey(), pinList, isLastTier)) {
          oldestTime = accessTime.getValue();
          blockId = accessTime.getKey();
        }
      }
    }
    // }
    // }
    return new Pair<Long, Long>(blockId, oldestTime);
  }
}
