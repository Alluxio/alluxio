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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import tachyon.Pair;
import tachyon.worker.hierarchy.StorageDir;

/**
 * it is used to evict old blocks among several storage dirs by LRU.
 */
public class EvictLRU extends EvictLRUBase {

  public EvictLRU(StorageDir[] storageDirs) {
    super(storageDirs);
  }

  @Override
  public int getDirCandidate(List<BlockEvictionInfo> blockEvictInfoList, Set<Integer> pinList,
      boolean isLastTier, long requestSize) throws IOException {
    Map<Integer, Pair<Long, Long>> dir2LRUBlocks = new HashMap<Integer, Pair<Long, Long>>();
    Map<Integer, Set<Long>> dir2BlocksToEvict = new HashMap<Integer, Set<Long>>();
    Map<Integer, Long> sizeToEvict = new HashMap<Integer, Long>();
    while (true) {
      Pair<Integer, Long> candidate =
          getLRUBlockCandidate(dir2LRUBlocks, dir2BlocksToEvict, pinList, isLastTier);
      int dirIndex = candidate.getFirst();
      long blockId = candidate.getSecond();
      long blockSize = 0;
      if (dirIndex == -1) {
        throw new IOException("no block can be evicted in current tier!");
      } else {
        blockSize = mStorageDirs[dirIndex].getBlockSize(blockId);
      }
      blockEvictInfoList.add(new BlockEvictionInfo(dirIndex, blockId, blockSize));
      Set<Long> blocksToEvict;
      if (dir2BlocksToEvict.containsKey(dirIndex)) {
        blocksToEvict = dir2BlocksToEvict.get(dirIndex);
      } else {
        blocksToEvict = new HashSet<Long>();
        dir2BlocksToEvict.put(dirIndex, blocksToEvict);
      }
      blocksToEvict.add(blockId);
      dir2LRUBlocks.remove(dirIndex);
      long evictionSize;
      if (sizeToEvict.containsKey(dirIndex)) {
        evictionSize = sizeToEvict.get(dirIndex) + blockSize;
      } else {
        evictionSize = blockSize;
      }
      sizeToEvict.put(dirIndex, evictionSize);
      if (evictionSize + mStorageDirs[dirIndex].getAvailable() >= requestSize) {
        return dirIndex;
      }
    }
  }

  /**
   * get block to be evicted
   * 
   * @param dir2LRUBlocks
   *          oldest access information for each storage dir
   * @param dir2BlocksToEvict
   *          block ids that already selected to be evicted
   * @param pinList
   *          list of pinned files
   * @param isLastTier
   *          whether current storage tier is the last tier
   * @return block to be evicted
   */
  public Pair<Integer, Long> getLRUBlockCandidate(Map<Integer, Pair<Long, Long>> dir2LRUBlocks,
      Map<Integer, Set<Long>> dir2BlocksToEvict, Set<Integer> pinList, boolean isLastTier) {
    int dirIndex = -1;
    long blockId = -1;
    for (int index = 0; index < mStorageDirs.length; index ++) {
      Pair<Long, Long> lruBlock;
      long oldestTime = Long.MAX_VALUE;
      if (!dir2LRUBlocks.containsKey(index)) {
        Set<Long> blocksToEvict;
        if (dir2BlocksToEvict.containsKey(index)) {
          blocksToEvict = dir2BlocksToEvict.get(index);
        } else {
          blocksToEvict = new HashSet<Long>();
        }
        lruBlock = getLRUBlock(mStorageDirs[index], blocksToEvict, pinList, isLastTier);
        if (lruBlock.getFirst() != -1) {
          dir2LRUBlocks.put(index, lruBlock);
        } else {
          continue;
        }
      } else {
        lruBlock = dir2LRUBlocks.get(index);
      }
      if (lruBlock.getSecond() < oldestTime) {
        blockId = lruBlock.getFirst();
        oldestTime = lruBlock.getSecond();
        dirIndex = index;
      }
    }
    return new Pair<Integer, Long>(dirIndex, blockId);
  }
}
