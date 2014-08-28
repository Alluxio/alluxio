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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;

import tachyon.Pair;
import tachyon.worker.hierarchy.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * It is used to evict old blocks among several storage dirs by LRU.
 */
public class EvictLRU extends EvictLRUBase {

  public EvictLRU(boolean lastTier) {
    super(lastTier);
  }

  @Override
  public StorageDir getDirCandidate(List<BlockInfo> blockEvictInfoList, StorageDir[] storageDirs,
      Set<Integer> pinList, long requestSize) {
    Map<StorageDir, Pair<Long, Long>> dir2LRUBlocks = new HashMap<StorageDir, Pair<Long, Long>>();
    HashMultimap<StorageDir, Long> dir2BlocksToEvict = HashMultimap.create();
    Map<StorageDir, Long> sizeToEvict = new HashMap<StorageDir, Long>();
    while (true) {
      Pair<StorageDir, Long> candidate =
          getLRUBlockCandidate(storageDirs, dir2LRUBlocks, dir2BlocksToEvict, pinList);
      StorageDir dirCandidate = candidate.getFirst();
      long blockId = candidate.getSecond();
      long blockSize = 0;
      if (dirCandidate == null) {
        return null;
      } else {
        blockSize = dirCandidate.getBlockSize(blockId);
      }
      blockEvictInfoList.add(new BlockInfo(dirCandidate, blockId, blockSize));
      dir2BlocksToEvict.put(dirCandidate, blockId);
      dir2LRUBlocks.remove(dirCandidate);
      long evictionSize;
      if (sizeToEvict.containsKey(dirCandidate)) {
        evictionSize = sizeToEvict.get(dirCandidate) + blockSize;
      } else {
        evictionSize = blockSize;
      }
      sizeToEvict.put(dirCandidate, evictionSize);
      if (evictionSize + dirCandidate.getAvailable() >= requestSize) {
        return dirCandidate;
      }
    }
  }

  /**
   * Get block to be evicted by choosing the oldest block in current StorageDirs
   * 
   * @param storageDirs
   *          storage dirs that the space is allocated in
   * @param dir2LRUBlocks
   *          oldest access information for each storage dir
   * @param dir2BlocksToEvict
   *          block ids that already selected to be evicted
   * @param pinList
   *          list of pinned files
   * @return block to be evicted
   */
  public Pair<StorageDir, Long> getLRUBlockCandidate(StorageDir[] storageDirs,
      Map<StorageDir, Pair<Long, Long>> dir2LRUBlocks,
      HashMultimap<StorageDir, Long> dir2BlocksToEvict, Set<Integer> pinList) {
    StorageDir dirCandidate = null;
    long blockId = -1;
    for (StorageDir dir : storageDirs) {
      Pair<Long, Long> lruBlock;
      long oldestTime = Long.MAX_VALUE;
      if (!dir2LRUBlocks.containsKey(dir)) {
        Set<Long> blocksToEvict = dir2BlocksToEvict.get(dir);
        lruBlock = getLRUBlock(dir, blocksToEvict, pinList);
        if (lruBlock.getFirst() != -1) {
          dir2LRUBlocks.put(dir, lruBlock);
        } else {
          continue;
        }
      } else {
        lruBlock = dir2LRUBlocks.get(dir);
      }
      if (lruBlock.getSecond() < oldestTime) {
        blockId = lruBlock.getFirst();
        oldestTime = lruBlock.getSecond();
        dirCandidate = dir;
      }
    }
    return new Pair<StorageDir, Long>(dirCandidate, blockId);
  }
}
