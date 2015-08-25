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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.exception.NotFoundException;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.StorageTierView;

/**
 * This class is used to evict old blocks in certain StorageDir by LRU. The main difference
 * between PartialLRU and LRU is that LRU choose old blocks among several StorageDirs
 * until one StorageDir satisfies the request space, but PartialLRU select one StorageDir
 * with maximum free space first and evict old blocks in the selected StorageDir by LRU
 */
public class PartialLRUEvictor extends LRUEvictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final Allocator mAllocator;
  private BlockMetadataManagerView mManagerView;

  public PartialLRUEvictor(BlockMetadataManagerView view, Allocator allocator) {
    super(view, allocator);
    mManagerView = view;
    mAllocator = allocator;
  }

  @Override
  protected StorageDirView cascadingEvict(long bytesToBeAvailable, BlockStoreLocation location,
      EvictionPlan plan) {

    // 1. Get StorageDir with max free space. If no such StorageDir, return null. If
    // bytesToBeAvailable can already be satisfied without eviction, return emtpy plan
    StorageDirView candidateDirView = getDirWithMaxFreeSpace(bytesToBeAvailable, location);
    if (candidateDirView == null || candidateDirView.getAvailableBytes() >= bytesToBeAvailable) {
      return candidateDirView;
    }

    // 2. iterate over blocks in LRU order until the candidate StorageDir can satisfy
    // bytesToBeAvailable after evicting its blocks iterated so far
    List<Long> candidateBlocks = new ArrayList<Long>();
    long freedBytes = 0;
    Iterator<Map.Entry<Long, Boolean>> it = mLRUCache.entrySet().iterator();
    while (it.hasNext() && candidateDirView.getAvailableBytes() + freedBytes < bytesToBeAvailable) {
      long blockId = it.next().getKey();
      try {
        BlockMeta block = mManagerView.getBlockMeta(blockId);
        if (null != block) { // might not present in this view
          if (block.getBlockLocation().belongTo(candidateDirView.toBlockStoreLocation())) {
            freedBytes += block.getBlockSize();
            candidateBlocks.add(block.getBlockId());
          }
        }
      } catch (NotFoundException nfe) {
        LOG.warn("Remove block {} from LRU Cache because {}", blockId, nfe);
        it.remove();
      }
    }

    // 3. have no eviction plan
    if (candidateDirView.getAvailableBytes() + freedBytes < bytesToBeAvailable) {
      return null;
    }

    // 4. cascading eviction: try to free space in next tier to move candidate blocks there, evict
    // blocks only when it can not be moved to next tiers
    List<StorageTierView> tierViewsBelow =
        mManagerView.getTierViewsBelow(candidateDirView.getParentTierView().getTierViewAlias());
    // find a dir in below tiers to transfer blocks there, from top tier to bottom tier
    StorageDirView candidateNextDir = null;
    for (StorageTierView tierView : tierViewsBelow) {
      candidateNextDir =
          cascadingEvict(freedBytes,
              BlockStoreLocation.anyDirInTier(tierView.getTierViewAlias()), plan);
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
    return candidateDirView;
  }

  /**
   * Get StorageDirView with max free space.
   *
   * @param availableBytes space size to be requested
   * @param location location that the space will be allocated in
   * @return the StorageDirView selected
   * @throws IOException
   */
  private StorageDirView getDirWithMaxFreeSpace(long availableBytes, BlockStoreLocation location) {
    long maxFreeSize = -1;
    StorageDirView selectedDirView = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getCommittedBytes() + dirView.getAvailableBytes() >= availableBytes
              && dirView.getAvailableBytes() > maxFreeSize) {
            selectedDirView = dirView;
            maxFreeSize = dirView.getAvailableBytes();
          }
        }
      }
    } else {
      int tierAlias = location.tierAlias();
      StorageTierView tierView = mManagerView.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getCommittedBytes() + dirView.getAvailableBytes() >= availableBytes
              && dirView.getAvailableBytes() > maxFreeSize) {
            selectedDirView = dirView;
            maxFreeSize = dirView.getAvailableBytes();
          }
        }
      } else {
        int dirIndex = location.dir();
        StorageDirView dirView = tierView.getDirView(dirIndex);
        if (dirView.getCommittedBytes() + dirView.getAvailableBytes() >= availableBytes
            && dirView.getAvailableBytes() > maxFreeSize) {
          selectedDirView = dirView;
          maxFreeSize = dirView.getAvailableBytes();
        }
      }
    }
    return selectedDirView;
  }
}
