package tachyon.worker.eviction;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import tachyon.Pair;
import tachyon.master.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Base class for evicting blocks by LRU strategy.
 */
public abstract class EvictLRUBase implements EvictStrategy {

  private final boolean mLastTier;

  EvictLRUBase(boolean lastTier) {
    mLastTier = lastTier;
  }

  /**
   * Check if current block can be evicted
   * 
   * @param blockId Id of the block
   * @param pinList list of pinned files
   * @return true if the block can be evicted, false otherwise
   */
  boolean blockEvictable(long blockId, Set<Integer> pinList) {
    if (mLastTier && pinList.contains(BlockInfo.computeInodeId(blockId))) {
      return false;
    }
    return true;
  }

  /**
   * Get the oldest access information of certain StorageDir
   * 
   * @param curDir current StorageDir
   * @param toEvictBlockIds Ids of blocks that have been selected to be evicted
   * @param pinList list of pinned files
   * @return the oldest access information of current StorageDir
   */
  Pair<Long, Long> getLRUBlock(StorageDir curDir, Collection<Long> toEvictBlockIds,
      Set<Integer> pinList) {
    long blockId = -1;
    long oldestTime = Long.MAX_VALUE;
    Set<Entry<Long, Long>> accessTimes = curDir.getLastBlockAccessTimeMs();

    for (Entry<Long, Long> accessTime : accessTimes) {
      if (toEvictBlockIds.contains(accessTime.getKey())) {
        continue;
      }
      if (accessTime.getValue() < oldestTime && !curDir.isBlockLocked(accessTime.getKey())) {
        if (blockEvictable(accessTime.getKey(), pinList)) {
          oldestTime = accessTime.getValue();
          blockId = accessTime.getKey();
        }
      }
    }

    return new Pair<Long, Long>(blockId, oldestTime);
  }
}
