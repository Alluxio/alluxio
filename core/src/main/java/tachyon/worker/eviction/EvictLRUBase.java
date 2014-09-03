package tachyon.worker.eviction;

import java.util.Collection;
import java.util.Map;
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
   * @param blockId
   *          id of the block
   * @param pinList
   *          list of pinned files
   * @return true if the block can be evicted, false otherwise
   */
  boolean blockEvictable(long blockId, Set<Integer> pinList) {
    if (mLastTier && pinList.contains(BlockInfo.computeInodeId(blockId))) {
      return false;
    }
    return true;
  }

  /**
   * Get the oldest block access information in certain StorageDir
   * 
   * @param curDir
   *          current StorageDir
   * @param toEvictBlockIds
   *          block ids that have been selected to be evicted
   * @param pinList
   *          list of pinned files
   * @return oldest access information of current StorageDir
   */
  Pair<Long, Long> getLRUBlock(StorageDir curDir, Collection<Long> toEvictBlockIds,
      Set<Integer> pinList) {
    long blockId = -1;
    long oldestTime = Long.MAX_VALUE;
    Map<Long, Long> accessTimes = curDir.getLastBlockAccessTime();

    for (Entry<Long, Long> accessTime : accessTimes.entrySet()) {
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
