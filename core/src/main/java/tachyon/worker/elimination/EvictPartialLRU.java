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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.Pair;
import tachyon.worker.hierarchy.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * It is used to evict blocks in certain storage dir by LRU strategy.
 */
public class EvictPartialLRU extends EvictLRUBase {

  public EvictPartialLRU(boolean lastTier) {
    super(lastTier);
  }

  @Override
  public StorageDir getDirCandidate(List<BlockInfo> blockEvictionInfoList,
      StorageDir[] storageDirs, Set<Integer> pinList, long requestSize) {
    Set<StorageDir> ignoredDirs = new HashSet<StorageDir>();
    StorageDir dirSelected = getDirWithMaxFreeSpace(requestSize, storageDirs, ignoredDirs);
    while (dirSelected != null) {
      Set<Long> blockIdSet = new HashSet<Long>();
      long sizeToEvict = 0;
      while (sizeToEvict + dirSelected.getAvailable() < requestSize) {
        Pair<Long, Long> oldestAccess = getLRUBlock(dirSelected, blockIdSet, pinList);
        if (oldestAccess.getFirst() != -1) {
          long blockSize = dirSelected.getBlockSize(oldestAccess.getFirst());
          sizeToEvict += blockSize;
          blockEvictionInfoList
              .add(new BlockInfo(dirSelected, oldestAccess.getFirst(), blockSize));
          blockIdSet.add(oldestAccess.getFirst());
        } else {
          break;
        }
      }
      if (sizeToEvict + dirSelected.getAvailable() < requestSize) {
        ignoredDirs.add(dirSelected);
        blockEvictionInfoList.clear();
        blockIdSet.clear();
        dirSelected = getDirWithMaxFreeSpace(requestSize, storageDirs, ignoredDirs);
      } else {
        return dirSelected;
      }
    }
    return null;
  }

  /**
   * Get the storage dir which has max free space
   * 
   * @param requestSize
   *          the space size to request
   * @param storageDirs
   *          storage dirs that the space is allocated in
   * @param ignoredList
   *          storage dirs that has ignored
   * @return the storage dir selected
   */
  public StorageDir getDirWithMaxFreeSpace(long requestSize, StorageDir[] storageDirs,
      Set<StorageDir> ignoredList) {
    StorageDir dirSelected = null;
    long maxAvailableSize = -1;
    for (StorageDir dir : storageDirs) {
      if (ignoredList.contains(dir)) {
        continue;
      }
      if (dir.getCapacity() >= requestSize && dir.getAvailable() > maxAvailableSize) {
        dirSelected = dir;
        maxAvailableSize = dir.getAvailable();
      }
    }
    return dirSelected;
  }
}
