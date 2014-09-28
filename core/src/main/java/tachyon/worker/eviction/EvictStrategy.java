package tachyon.worker.eviction;

import java.util.List;
import java.util.Set;

import tachyon.Pair;
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
   * @param storageDirs StorageDir candidates that the space will be allocated in
   * @param pinList list of pinned file
   * @param requestSize size to request
   * @return Pair of StorageDir allocated and blockInfoList which contains information of blocks to
   *         be evicted, null if no allocated directory is found
   */
  public Pair<StorageDir, List<BlockInfo>> getDirCandidate(StorageDir[] storageDirs,
      Set<Integer> pinList, long requestSize);
}
