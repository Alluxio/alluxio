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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.Pair;
import tachyon.worker.hierarchy.StorageDir;

/**
 * It is used to evict blocks in certain storage dir by LRU strategy.
 */
public class EvictPartialLRU extends EvictLRUBase {

  public EvictPartialLRU(StorageDir[] storageDirs, boolean lastTier) {
    super(storageDirs, lastTier);
  }

  @Override
  public StorageDir getDirCandidate(List<BlockEvictionInfo> blockEvictionInfoList,
      Set<Integer> pinList, long requestSize) throws IOException {
    Set<Integer> ignoredDirs = new HashSet<Integer>();
    int dirIndex = getDirWithMaxFreeSpace(requestSize, ignoredDirs);
    while (dirIndex != -1) {
      Set<Long> blockIdSet = new HashSet<Long>();
      long sizeToEvict = 0;
      while (sizeToEvict + STORAGE_DIRS[dirIndex].getAvailable() < requestSize) {
        Pair<Long, Long> oldestAccess = getLRUBlock(STORAGE_DIRS[dirIndex], blockIdSet, pinList);
        if (oldestAccess.getFirst() != -1) {
          long blockSize = STORAGE_DIRS[dirIndex].getBlockSizes().get(oldestAccess.getFirst());
          sizeToEvict += blockSize;
          blockEvictionInfoList.add(new BlockEvictionInfo(dirIndex, oldestAccess.getFirst(),
              blockSize));
          blockIdSet.add(oldestAccess.getFirst());
        } else {
          break;
        }
      }
      if (sizeToEvict + STORAGE_DIRS[dirIndex].getAvailable() < requestSize) {
        ignoredDirs.add(dirIndex);
        blockEvictionInfoList.clear();
        blockIdSet.clear();
        dirIndex = getDirWithMaxFreeSpace(requestSize, ignoredDirs);
      } else {
        return STORAGE_DIRS[dirIndex];
      }
    }
    throw new IOException("No suitable dir can be found!");
  }

  public int getDirWithMaxFreeSpace(long requestSize, Set<Integer> ignoredList) {
    int dirSelected = -1;
    long maxAvailableSize = -1;
    for (int index = 0; index < STORAGE_DIRS.length; index ++) {
      if (ignoredList.contains(index)) {
        continue;
      }
      if (STORAGE_DIRS[index].getCapacity() >= requestSize
          && STORAGE_DIRS[index].getAvailable() > maxAvailableSize) {
        dirSelected = index;
        maxAvailableSize = STORAGE_DIRS[index].getAvailable();
      }
    }
    return dirSelected;
  }
}
