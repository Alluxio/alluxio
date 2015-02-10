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

package tachyon.worker.eviction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.Pair;
import tachyon.worker.hierarchy.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Used to evict blocks in certain StorageDir by LRU strategy.
 */
public final class EvictPartialLRU extends EvictLRUBase {

  public EvictPartialLRU(boolean lastTier) {
    super(lastTier);
  }

  @Override
  public synchronized Pair<StorageDir, List<BlockInfo>> getDirCandidate(StorageDir[] storageDirs,
      Set<Integer> pinList, long requestBytes) {
    List<BlockInfo> blockInfoList = new ArrayList<BlockInfo>();
    Set<StorageDir> ignoredDirs = new HashSet<StorageDir>();
    StorageDir dirSelected = getDirWithMaxFreeSpace(requestBytes, storageDirs, ignoredDirs);
    while (dirSelected != null) {
      Set<Long> blockIdSet = new HashSet<Long>();
      long sizeToEvict = 0;
      while (sizeToEvict + dirSelected.getAvailableBytes() < requestBytes) {
        Pair<Long, Long> oldestAccess = getLRUBlock(dirSelected, blockIdSet, pinList);
        if (oldestAccess.getFirst() != -1) {
          long blockSize = dirSelected.getBlockSize(oldestAccess.getFirst());
          sizeToEvict += blockSize;
          blockInfoList.add(new BlockInfo(dirSelected, oldestAccess.getFirst(), blockSize));
          blockIdSet.add(oldestAccess.getFirst());
        } else {
          break;
        }
      }
      if (sizeToEvict + dirSelected.getAvailableBytes() < requestBytes) {
        ignoredDirs.add(dirSelected);
        blockInfoList.clear();
        blockIdSet.clear();
        dirSelected = getDirWithMaxFreeSpace(requestBytes, storageDirs, ignoredDirs);
      } else {
        return new Pair<StorageDir, List<BlockInfo>>(dirSelected, blockInfoList);
      }
    }
    return null;
  }

  /**
   * Get the StorageDir which has max free space
   * 
   * @param requestSize space size to request
   * @param storageDirs StorageDir candidates that the space will be allocated in
   * @param ignoredList StorageDirs that have been ignored
   * @return the StorageDir selected
   */
  private StorageDir getDirWithMaxFreeSpace(long requestSize, StorageDir[] storageDirs,
      Set<StorageDir> ignoredList) {
    StorageDir dirSelected = null;
    long maxAvailableSize = -1;
    for (StorageDir dir : storageDirs) {
      if (ignoredList.contains(dir)) {
        continue;
      }
      if (dir.getCapacityBytes() >= requestSize && dir.getAvailableBytes() > maxAvailableSize) {
        dirSelected = dir;
        maxAvailableSize = dir.getAvailableBytes();
      }
    }
    return dirSelected;
  }
}
