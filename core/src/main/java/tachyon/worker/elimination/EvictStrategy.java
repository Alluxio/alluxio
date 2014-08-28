package tachyon.worker.elimination;

import java.util.List;
import java.util.Set;

import tachyon.worker.hierarchy.BlockInfo;
import tachyon.worker.hierarchy.StorageDir;

/**
 * Used to get StorageDir which space is allocated in and blocks that will be evicted to get enough
 * space. because pin file / locked blocks information may be updated after candidate selection,
 * when actually evicting blocks, some blocks may be not allowed to be evicted, it may result in
 * having to try more than one time to get enough space.
 */
public interface EvictStrategy {

  /**
   * Get StorageDir allocated and also get blocks to be evicted among StorageDir candidates
   * 
   * @param blockInfoList
   *          information of blocks to be evicted
   * @param storageDirs
   *          StorageDir candidates that the space will be allocated in
   * @param pinList
   *          list of pinned file
   * @param requestSize
   *          size to request
   * @return StorageDir allocated, blockInfoList is also returned as output
   */
  StorageDir getDirCandidate(List<BlockInfo> blockInfoList, StorageDir[] storageDirs,
      Set<Integer> pinList, long requestSize);
}
