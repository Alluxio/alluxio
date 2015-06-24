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

package tachyon.worker.block.evictor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.Pair;
import tachyon.worker.BlockStoreLocation;

/**
 * A collection of candidate blocks for eviction organized by directory
 */
class EvictionCandidates {
  private Map<BlockStoreLocation, Pair<List<Long>, Long>> mDirCandidates =
      new HashMap<BlockStoreLocation, Pair<List<Long>, Long>>();
  private long mMaxAvailableBytes = 0;
  private BlockStoreLocation mDirWithMaxAvailableBytes = null;

  /**
   * Add the block in the directory to this collection
   *
   * @param dir the directory where the block resides
   * @param blockId blockId of the block
   * @param blockSizeBytes block size in bytes
   */
  public void add(BlockStoreLocation dir, long blockId, long blockSizeBytes) {
    Pair<List<Long>, Long> candidate;
    if (mDirCandidates.containsKey(dir)) {
      candidate = mDirCandidates.get(dir);
    } else {
      candidate = new Pair<List<Long>, Long>(new ArrayList<Long>(), 0L);
      mDirCandidates.put(dir, candidate);
    }

    candidate.getFirst().add(blockId);
    long evictBytes = candidate.getSecond() + blockSizeBytes;
    candidate.setSecond(evictBytes);

    if (mMaxAvailableBytes < evictBytes) {
      mMaxAvailableBytes = evictBytes;
      mDirWithMaxAvailableBytes = dir;
    }
  }

  /**
   * The maximum available bytes in one directory of all candidate directories
   *
   * @return maximum available bytes
   */
  public long maxAvailableBytes() {
    return mMaxAvailableBytes;
  }

  /**
   * The directory that has maximum available bytes
   *
   * @return the directory
   */
  public BlockStoreLocation dirWithMaxAvailableBytes() {
    return mDirWithMaxAvailableBytes;
  }

  /**
   * List of blockIds for eviction
   *
   * @return list of blockIds
   */
  public List<Long> toEvict() {
    return mDirCandidates.get(mDirWithMaxAvailableBytes).getFirst();
  }
}
