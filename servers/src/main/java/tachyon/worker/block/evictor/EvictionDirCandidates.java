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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockStoreLocation;

/**
 * A collection of candidate blocks for eviction organized by directory
 */
class EvictionDirCandidates {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Map from StorageDirId to pair of list of candidate blockIds and their total size in bytes */
  private Map<Long, Pair<List<Long>, Long>> mDirCandidates =
      new HashMap<Long, Pair<List<Long>, Long>>();
  private long mMaxAvailableBytes = 0;
  private Long mKeyOfMaxAvailableBytes = null;

  /**
   * Add the block in the directory to this collection
   *
   * @param dir the directory where the block resides
   * @param blockId blockId of the block
   * @param blockSizeBytes block size in bytes
   */
  public void add(BlockStoreLocation dir, long blockId, long blockSizeBytes) {
    LOG.debug("add block {} with bytes {} to dir {}", blockId, blockSizeBytes, dir);
    Pair<List<Long>, Long> candidate;
    long dirId = dir.getStorageDirId();
    if (mDirCandidates.containsKey(dirId)) {
      LOG.debug("dir already in mDirCandidates");
      candidate = mDirCandidates.get(dirId);
    } else {
      LOG.debug("new dir, put to mDirCandidates");
      candidate = new Pair<List<Long>, Long>(new ArrayList<Long>(), 0L);
      mDirCandidates.put(dirId, candidate);
    }

    candidate.getFirst().add(blockId);
    long evictBytes = candidate.getSecond() + blockSizeBytes;
    LOG.debug("maxAvailableBytes = {}, evictBytes = {}", mMaxAvailableBytes, evictBytes);
    candidate.setSecond(evictBytes);

    if (mMaxAvailableBytes < evictBytes) {
      mMaxAvailableBytes = evictBytes;
      mKeyOfMaxAvailableBytes = dirId;
    }
    LOG.debug("mDirCandidates.size() = {}", mDirCandidates.size());
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
   * List of blockIds for eviction
   *
   * @return list of blockIds
   */
  public List<Long> toEvict() {
    Pair<List<Long>, Long> evict = mDirCandidates.get(mKeyOfMaxAvailableBytes);
    if (evict == null) {
      return new ArrayList<Long>();
    }
    return evict.getFirst();
  }
}
