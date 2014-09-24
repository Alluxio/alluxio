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
      Set<Integer> pinList, long requestSize) {
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
      StorageDir dirCandidate = candidate.getFirst();
      long blockId = candidate.getSecond();
      long blockSize = 0;
      if (dirCandidate == null) {
        return null;
      } else {
        blockSize = dirCandidate.getBlockSize(blockId);
      }
      // Add info of the block to the list
      blockInfoList.add(new BlockInfo(dirCandidate, blockId, blockSize));
      dir2BlocksToEvict.put(dirCandidate, blockId);
      dir2LRUBlocks.remove(dirCandidate);
      long evictionSize;
      // Update eviction size for this StorageDir
      if (sizeToEvict.containsKey(dirCandidate)) {
        evictionSize = sizeToEvict.get(dirCandidate) + blockSize;
      } else {
        evictionSize = blockSize;
      }
      sizeToEvict.put(dirCandidate, evictionSize);
      if (evictionSize + dirCandidate.getAvailableBytes() >= requestSize) {
        return new Pair<StorageDir, List<BlockInfo>>(dirCandidate, blockInfoList);
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
