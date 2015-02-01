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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;

import tachyon.Pair;
import tachyon.worker.hierarchy.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Used to evict old blocks among several StorageDirs by LRU strategy.
 */
public final class EvictLRU extends EvictLRUBase {

  public EvictLRU(boolean lastTier) {
    super(lastTier);
  }

  @Override
  public synchronized Pair<StorageDir, List<BlockInfo>> getDirCandidate(StorageDir[] storageDirs,
      Set<Integer> pinList, long requestBytes) {
    List<BlockInfo> blockInfoList = new ArrayList<BlockInfo>();
    Map<StorageDir, Pair<Long, Long>> dir2LRUBlocks = new HashMap<StorageDir, Pair<Long, Long>>();
    HashMultimap<StorageDir, Long> dir2BlocksToEvict = HashMultimap.create();
    Map<StorageDir, Long> sizeToEvict = new HashMap<StorageDir, Long>();
    // If no StorageDir has enough space for the request size, continue; if no block can be evicted,
    // return null; and if eviction size plus free space of some StorageDir is larger than request
    // size, return the Pair of StorageDir and blockInfoList
    while (true) {
      // Get oldest block in StorageDir candidates
      Pair<StorageDir, Long> candidate =
          getLRUBlockCandidate(storageDirs, dir2LRUBlocks, dir2BlocksToEvict, pinList);
      StorageDir dir = candidate.getFirst();
      if (dir == null) {
        return null;
      }
      long blockId = candidate.getSecond();
      long blockSize = dir.getBlockSize(blockId);
      // Add info of the block to the list
      blockInfoList.add(new BlockInfo(dir, blockId, blockSize));
      dir2BlocksToEvict.put(dir, blockId);
      dir2LRUBlocks.remove(dir);
      long evictBytes;
      // Update eviction size for this StorageDir
      if (sizeToEvict.containsKey(dir)) {
        evictBytes = sizeToEvict.get(dir) + blockSize;
      } else {
        evictBytes = blockSize;
      }
      sizeToEvict.put(dir, evictBytes);
      if (evictBytes + dir.getAvailableBytes() >= requestBytes) {
        return new Pair<StorageDir, List<BlockInfo>>(dir, blockInfoList);
      }
    }
  }

  /**
   * Get block to be evicted by choosing the oldest block in StorageDir candidates
   * 
   * @param storageDirs StorageDir candidates that the space will be allocated in
   * @param dir2LRUBlocks the oldest access information of each StorageDir
   * @param dir2BlocksToEvict Ids of blocks that have been selected to be evicted
   * @param pinList list of pinned files
   * @return pair of StorageDir that contains the block to be evicted and Id of the block
   */
  private Pair<StorageDir, Long> getLRUBlockCandidate(StorageDir[] storageDirs,
      Map<StorageDir, Pair<Long, Long>> dir2LRUBlocks,
      HashMultimap<StorageDir, Long> dir2BlocksToEvict, Set<Integer> pinList) {
    StorageDir dirCandidate = null;
    long blockId = -1;
    long oldestTime = Long.MAX_VALUE;
    for (StorageDir dir : storageDirs) {
      Pair<Long, Long> lruBlock;
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
