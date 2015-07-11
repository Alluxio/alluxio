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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * This class is used to evict old blocks in certain StorageDir by LRU. The main difference
 * between PartialLRU and LRU is that LRU choose old blocks among several StorageDirs
 * until one StorageDir satisfies the request space, but PartialLRU select one StorageDir
 * first and evict old blocks in certain StorageDir by LRU
 */
public class PartialLRUEvictor extends LRUEvictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE); 
  private BlockMetadataManager mMetaManager;

  public PartialLRUEvictor(BlockMetadataManager meta) {
    super(meta);
    mMetaManager = Preconditions.checkNotNull(meta);
  }
  
  @Override
  protected StorageDir cascadingEvict(long bytesToBeAvailable, BlockStoreLocation location,
      EvictionPlan plan) throws IOException {

    StorageDir candidateDir = null;
    // 1. Get StorageDir with max free space. If no such StorageDir, return null. If
    // bytesToBeAvailable can already be satisfied without eviction, return emtpy plan
    candidateDir = getDirWithMaxFreeSpace(bytesToBeAvailable, location);
    if (candidateDir == null || candidateDir.getAvailableBytes() >= bytesToBeAvailable) {
      return candidateDir;
    }

    // 2. iterate over blocks in LRU order until the candidate StorageDir can satisfy
    // bytesToBeAvailable after evicting its blocks iterated so far
    List<Long> candidateBlocks = new ArrayList<Long>();
    long freedBytes = 0;
    Iterator<Map.Entry<Long, Boolean>> it = mLRUCache.entrySet().iterator();
    while (it.hasNext() && candidateDir.getAvailableBytes() + freedBytes < bytesToBeAvailable) {
      long blockId = it.next().getKey();
      try {
        BlockMeta block = mMetaManager.getBlockMeta(blockId);
        if (block.getParentDir().getStorageDirId() == candidateDir.getStorageDirId()) {
          candidateBlocks.add(block.getBlockId());
          freedBytes += block.getBlockSize();
        }
      } catch (IOException ioe) {
        LOG.warn("Remove block {} from LRU Cache because {}", blockId, ioe);
        it.remove();
      }
    }

    // 3. have no eviction plan
    if (candidateDir.getAvailableBytes() + freedBytes < bytesToBeAvailable) {
      return null;
    }

    // 4. cascading eviction: try to free space in next tier to move candidate blocks there, evict
    // blocks only when it can not be moved to next tiers
    List<StorageTier> tiersBelow =
        mMetaManager.getTiersBelow(candidateDir.getParentTier().getTierAlias());
    // find a dir in below tiers to transfer blocks there, from top tier to bottom tier
    StorageDir candidateNextDir = null;
    for (StorageTier tier : tiersBelow) {
      candidateNextDir =
          cascadingEvict(freedBytes,
              BlockStoreLocation.anyDirInTier(tier.getTierAlias()), plan);
      if (candidateNextDir != null) {
        break;
      }
    }
    if (candidateNextDir == null) {
      // nowhere to transfer blocks to, so evict them
      plan.toEvict().addAll(candidateBlocks);
    } else {
      BlockStoreLocation dest = candidateNextDir.toBlockStoreLocation();
      for (long block : candidateBlocks) {
        plan.toMove().add(new Pair<Long, BlockStoreLocation>(block, dest));
      }
    }
    return candidateDir;
  }

  /**
   * Get StorageDir with max free space.
   * 
   * @param availableBytes space size to be requested
   * @param location location that the space will be allocated in
   * @return the StorageDir selected
   * @throws IOException
   */
  private StorageDir getDirWithMaxFreeSpace(long availableBytes, BlockStoreLocation location)
      throws IOException {
    long maxFreeSize = -1;
    StorageDir selectedDir = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : mMetaManager.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
              && dir.getAvailableBytes() > maxFreeSize) {
            selectedDir = dir;
            maxFreeSize = dir.getAvailableBytes();
          }
        }
      }
    } else {
      int tierAlias = location.tierAlias();
      StorageTier tier = mMetaManager.getTier(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        // Loop over all dirs in the given tier
        for (StorageDir dir : tier.getStorageDirs()) {
          if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
              && dir.getAvailableBytes() > maxFreeSize) {
            selectedDir = dir;
            maxFreeSize = dir.getAvailableBytes();
          }
        }
      } else {
        int dirIndex = location.dir();
        StorageDir dir = tier.getDir(dirIndex);
        if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
            && dir.getAvailableBytes() > maxFreeSize) {
          selectedDir = dir;
          maxFreeSize = dir.getAvailableBytes();
        }
      }
    }
    return selectedDir;
  }
}
