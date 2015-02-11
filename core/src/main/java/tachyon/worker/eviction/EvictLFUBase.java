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
import java.util.Set;
import java.util.Map.Entry;

import tachyon.Pair;
import tachyon.master.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Base class for evicting blocks by LFU strategy.
 */
public abstract class EvictLFUBase implements EvictStrategy {
  private final boolean mLastTier;

  protected EvictLFUBase(boolean lastTier) {
    mLastTier = lastTier;
  }

  /**
   * Check if current block can be evicted
   * 
   * @param blockId the Id of the block
   * @param pinList the list of the pinned files
   * @return true if the block can be evicted, false otherwise
   */
  protected boolean blockEvictable(long blockId, Set<Integer> pinList) {
    return !mLastTier || !pinList.contains(BlockInfo.computeInodeId(blockId));
  }

  /**
   * Get the least frequency information of certain StorageDir
   * 
   * @param curDir current StorageDir
   * @param toEvictBlockIds the Ids of blocks that have been selected to be evicted
   * @param pinList list of pinned files
   * @return the least frequency information of current StorageDir
   */
  protected Pair<Long, Long> getLFUBlock(StorageDir curDir, Collection<Long> toEvictBlockIds,
      Set<Integer> pinList) {
    long blockId = -1;
    long leastFreq = Long.MAX_VALUE;
    Set<Entry<Long, Long>> refFreqs = curDir.getBlockReferenceFrequency();
    for (Entry<Long, Long> refFreq : refFreqs) {
      if (toEvictBlockIds.contains(refFreq.getKey())) {
        continue;
      }
      if (refFreq.getValue() < leastFreq && !curDir.isBlockLocked(refFreq.getKey())) {
        if (blockEvictable(refFreq.getKey(), pinList)) {
          leastFreq = refFreq.getValue();
          blockId = refFreq.getKey();
        }
      }
    }
    return new Pair<Long, Long>(blockId, leastFreq);
  }
}
