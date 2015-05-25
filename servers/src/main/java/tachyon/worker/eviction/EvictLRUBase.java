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

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import tachyon.Pair;
import tachyon.worker.tiered.BlockInfo;
import tachyon.worker.tiered.StorageDir;

/**
 * Base class for evicting blocks by LRU strategy.
 */
public abstract class EvictLRUBase implements EvictStrategy {
  private final boolean mLastTier;
  protected final Multimap<StorageDir, Long> mEvictingBlockIds;

  protected EvictLRUBase(boolean lastTier) {
    mLastTier = lastTier;
    mEvictingBlockIds = Multimaps
        .synchronizedMultimap(HashMultimap.<StorageDir, Long>create());
  }

  /**
   * Check if the current block can be evicted
   *
   * @param blockId the Id of the block
   * @param pinList the list of the pinned files
   * @return true if the block can be evicted, false otherwise
   */
  protected boolean blockEvictable(long blockId, Set<Integer> pinList) {
    return !mLastTier || !pinList.contains(tachyon.master.BlockInfo.computeInodeId(blockId));
  }

  /**
   * Clean the Id of blocks that have been evicted from evicting list in each StorageDir
   */
  protected void cleanEvictingBlockIds() {
    for (Entry<StorageDir, Long> block : mEvictingBlockIds.entries()) {
      StorageDir dir = block.getKey();
      long blockId = block.getValue();
      // If the block has been evicted, remove it from list
      if (!dir.containsBlock(blockId)) {
        mEvictingBlockIds.remove(dir, blockId);
      }
    }
  }

  /**
   * Update the Id of blocks that are being evicted in each StorageDir
   * 
   * @param blockList The list of blocks that will be added in to evicting list
   */
  protected void updateEvictingBlockIds(Collection<BlockInfo> blockList) {
    for (BlockInfo block : blockList) {
      StorageDir dir = block.getStorageDir();
      long blockId = block.getBlockId();
      mEvictingBlockIds.put(dir, blockId);
    }
  }

  /**
   * Get the oldest access information of a StorageDir
   *
   * @param curDir current StorageDir
   * @param toEvictBlockIds the Ids of blocks that have been selected to evict
   * @param pinList list of pinned files
   * @return the oldest access information of current StorageDir
   */
  protected Pair<Long, Long> getLRUBlock(StorageDir curDir, Collection<Long> toEvictBlockIds,
      Set<Integer> pinList) {
    long lruBlockId = -1;
    long oldestTime = Long.MAX_VALUE;
    Set<Long> evictingBlockIds = (Set<Long>) mEvictingBlockIds.get(curDir);

    for (Entry<Long, Long> accessInfo : curDir.getLastBlockAccessTimeMs()) {
      long blockId = accessInfo.getKey();
      long accessTime = accessInfo.getValue();
      if (toEvictBlockIds.contains(blockId) || evictingBlockIds.contains(blockId)) {
        continue;
      }
      if (accessTime < oldestTime && !curDir.isBlockLocked(blockId)
          && blockEvictable(blockId, pinList)) {
        lruBlockId = blockId;
        oldestTime = accessTime;
      }
    }

    return new Pair<Long, Long>(lruBlockId, oldestTime);
  }
}
