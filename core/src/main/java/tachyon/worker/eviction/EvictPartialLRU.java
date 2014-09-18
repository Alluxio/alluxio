package tachyon.worker.eviction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.Pair;
import tachyon.worker.hierarchy.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Used to evict blocks in certain StorageDir by LRU strategy.
 */
public final class EvictPartialLRU extends EvictLRUBase {

  public EvictPartialLRU(boolean lastTier) {
    super(lastTier);
  }

  @Override
  public synchronized Pair<StorageDir, List<BlockInfo>> getDirCandidate(StorageDir[] storageDirs,
      Set<Integer> pinList, long requestSize) {
    List<BlockInfo> blockInfoList = new ArrayList<BlockInfo>();
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
          blockInfoList.add(new BlockInfo(dirSelected, oldestAccess.getFirst(), blockSize));
          blockIdSet.add(oldestAccess.getFirst());
        } else {
          break;
        }
      }
      if (sizeToEvict + dirSelected.getAvailable() < requestSize) {
        ignoredDirs.add(dirSelected);
        blockInfoList.clear();
        blockIdSet.clear();
        dirSelected = getDirWithMaxFreeSpace(requestSize, storageDirs, ignoredDirs);
      } else {
        return new Pair<StorageDir, List<BlockInfo>>(dirSelected, blockInfoList);
      }
    }
    return null;
  }

  /**
   * Get the StorageDir which has max free space
   *
   * @param requestSize space size to request
   * @param storageDirs StorageDir candidates that the space will be allocated in
   * @param ignoredList StorageDirs that have been ignored
   * @return the StorageDir selected
   */
  private StorageDir getDirWithMaxFreeSpace(long requestSize, StorageDir[] storageDirs,
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
