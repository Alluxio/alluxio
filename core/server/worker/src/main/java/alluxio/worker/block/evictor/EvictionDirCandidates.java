/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.collections.Pair;
import alluxio.worker.block.meta.StorageDirEvictorView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A collection of candidate blocks for eviction organized by directory.
 *
 * This class lets you add blocks with the {@link alluxio.worker.block.meta.StorageDir}s they reside
 * in, the blockIds and the sizes in bytes, then it tells you which StorageDir added so far has the
 * maximum sum of available bytes and total bytes of added blocks. Assume metadata of StorageDir
 * will not be changed during adding blocks.
 *
 * Example usage can be found in {@link LRUEvictor#freeSpaceWithView}.
 */
@NotThreadSafe
class EvictionDirCandidates {
  /**
   * Map from {@link StorageDirEvictorView} to pair of list of candidate blockIds
   * and their total size in bytes.
   */
  private Map<StorageDirEvictorView, Pair<List<Long>, Long>> mDirCandidates = new HashMap<>();
  /** Maximum sum of available bytes in a StorageDir and all its added blocks. */
  private long mMaxBytes = 0;
  private StorageDirEvictorView mDirWithMaxBytes = null;

  /**
   * Constructs a new {@link EvictionDirCandidates}.
   */
  public EvictionDirCandidates() {}

  /**
   * Adds the block in the directory to this collection.
   *
   * @param dir the dir where the block resides
   * @param blockId blockId of the block
   * @param blockSizeBytes block size in bytes
   */
  public void add(StorageDirEvictorView dir, long blockId, long blockSizeBytes) {
    Pair<List<Long>, Long> candidate;
    if (mDirCandidates.containsKey(dir)) {
      candidate = mDirCandidates.get(dir);
    } else {
      candidate = new Pair<List<Long>, Long>(new ArrayList<Long>(), 0L);
      mDirCandidates.put(dir, candidate);
    }

    candidate.getFirst().add(blockId);
    long blockBytes = candidate.getSecond() + blockSizeBytes;
    candidate.setSecond(blockBytes);

    long sum = blockBytes + dir.getAvailableBytes();
    if (mMaxBytes < sum) {
      mMaxBytes = sum;
      mDirWithMaxBytes = dir;
    }
  }

  /**
   * The maximum sum of available bytes and total bytes of added blocks in a directory.
   *
   * @return maximum bytes, if no directory has been added, return 0
   */
  public long candidateSize() {
    return mMaxBytes;
  }

  /**
   * @return list of blockIds in the directory that has the maximum {@link #candidateSize()},
   *         otherwise an empty list if no directory has been added
   */
  public List<Long> candidateBlocks() {
    Pair<List<Long>, Long> evict = mDirCandidates.get(mDirWithMaxBytes);
    if (evict == null) {
      return new ArrayList<>();
    }
    return evict.getFirst();
  }

  /**
   * @return the {@link alluxio.worker.block.meta.StorageDir} that has the maximum
   *         {@link #candidateSize()}, otherwise null if no directory has been added
   */
  public StorageDirEvictorView candidateDir() {
    return mDirWithMaxBytes;
  }
}
