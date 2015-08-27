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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.Users;
import tachyon.exception.NotFoundException;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;

public abstract class EvictorBase extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  protected final Allocator mAllocator;
  protected BlockMetadataManagerView mManagerView;

  public EvictorBase(BlockMetadataManagerView view, Allocator allocator) {
    mManagerView = Preconditions.checkNotNull(view);
    mAllocator = Preconditions.checkNotNull(allocator);
  }

  /**
   * A recursive implementation of cascading eviction.
   *
   * This method uses a specific eviction strategy to find blocks to evict in the requested
   * location. After eviction, one StorageDir in the location has the specific amount of free space.
   * It then uses an allocation strategy to allocate space in the next tier to move each evicted
   * blocks. If the next tier fails to allocate space for the evicted blocks, the next tier will
   * continue to evict its blocks to free space.
   *
   * This method is only used in {@link #freeSpaceWithView}.
   *
   * @param bytesToBeAvailable bytes to be available after eviction
   * @param location target location to evict blocks from
   * @param plan the plan to be recursively updated, is empty when first called in
   *        {@link #freeSpaceWithView}
   * @return the first StorageDirView in the range of location to evict/move bytes from, or null if
   *         there is no plan
   */
  protected StorageDirView cascadingEvict(long bytesToBeAvailable, BlockStoreLocation location,
      EvictionPlan plan) {
    location = updateBlockStoreLocation(bytesToBeAvailable, location);

    // 1. If bytesToBeAvailable can already be satisfied without eviction, return the eligible
    // StoargeDirView
    StorageDirView candidateDirView =
        EvictorUtils.selectDirWithRequestedSpace(bytesToBeAvailable, location, mManagerView);
    if (candidateDirView != null) {
      return candidateDirView;
    }

    // 2. Iterate over blocks in order until we find a StorageDirView that is in the range of
    // location and can satisfy bytesToBeAvailable after evicting its blocks iterated so far
    EvictionDirCandidates dirCandidates = new EvictionDirCandidates();
    Iterator<Map.Entry<Long, Object>> it = getBlockIterator();
    while (it.hasNext() && dirCandidates.candidateSize() < bytesToBeAvailable) {
      long blockId = it.next().getKey();
      try {
        BlockMeta block = mManagerView.getBlockMeta(blockId);
        if (null != block) { // might not present in this view
          if (block.getBlockLocation().belongTo(location)) {
            int tierAlias = block.getParentDir().getParentTier().getTierAlias();
            int dirIndex = block.getParentDir().getDirIndex();
            dirCandidates.add(mManagerView.getTierView(tierAlias).getDirView(dirIndex), blockId,
                block.getBlockSize());
          }
        }
      } catch (NotFoundException nfe) {
        LOG.warn("Remove block {} from evictor cache because {}", blockId, nfe);
        it.remove();
        onRemoveBlockFromIterator(blockId);
      }
    }

    // 3. If there is no eligible StorageDirView, return null
    if (dirCandidates.candidateSize() < bytesToBeAvailable) {
      return null;
    }

    // 4. cascading eviction: try to allocate space in the next tier to move candidate blocks
    // there. If allocation fails, the next tier will continue to evict its blocks to free space.
    // Blocks are only evicted from the last tier or it can not be moved to the next tier.
    candidateDirView = dirCandidates.candidateDir();
    List<Long> candidateBlocks = dirCandidates.candidateBlocks();
    StorageTierView nextTierView = mManagerView.getNextTier(candidateDirView.getParentTierView());
    if (nextTierView == null) {
      // This is the last tier, evict all the blocks.
      plan.toEvict().addAll(candidateBlocks);
      for (Long blockId : candidateBlocks) {
        try {
          BlockMeta block = mManagerView.getBlockMeta(blockId);
          if (null != block) {
            candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
          }
        } catch (NotFoundException nfe) {
          continue;
        }
      }
    } else {
      for (Long blockId : candidateBlocks) {
        try {
          BlockMeta block = mManagerView.getBlockMeta(blockId);
          if (null == block) {
            continue;
          }
          StorageDirView nextDirView =
              mAllocator.allocateBlockWithView(Users.MIGRATE_DATA_USER_ID, block.getBlockSize(),
                  BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()), mManagerView);
          if (nextDirView == null) {
            nextDirView =
                cascadingEvict(block.getBlockSize(),
                    BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()), plan);
          }
          if (nextDirView == null) {
            // If we failed to find a dir in the next tier to move this block, evict it and
            // continue. Normally this should not happen.
            plan.toEvict().add(blockId);
            candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
            continue;
          }
          plan.toMove().add(
              new Pair<Long, BlockStoreLocation>(blockId, nextDirView.toBlockStoreLocation()));
          candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
          nextDirView.markBlockMoveIn(blockId, block.getBlockSize());
        } catch (NotFoundException nfe) {
          continue;
        }
      }
    }

    return candidateDirView;
  }

  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataManagerView view) {
    mManagerView = view;

    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    EvictionPlan plan = new EvictionPlan(toMove, toEvict);
    StorageDirView candidateDir = cascadingEvict(bytesToBeAvailable, location, plan);

    mManagerView.clearBlockMarks();
    if (candidateDir == null) {
      return null;
    }

    return plan;
  }

  /**
   * @return an iterator over the blocks in the evictor cache. The evictor is responsible for
   * specifying the iteration order using its own strategy. For example, LRUEvictor returns an
   * iterator that iterates the blocks in LRU order. The key of the map entry is the block Id.
   */
  // TODO: Is a key iterator sufficient?
  protected abstract Iterator<Map.Entry<Long, Object>> getBlockIterator();

  /**
   * Perform additional cleanup when a block is removed from the iterator returned by
   * {@link #getBlockIterator}.
   */
  protected void onRemoveBlockFromIterator(long blockId) {}

  /**
   * Update the block store location if the evictor wants to free space in a specific location.
   * For example, PartialLRUEvictor always evicts blocks from a dir with max free space.
   *
   * @param bytesToBeAvailable bytes to be available after eviction
   * @param location the original block store location
   * @return the updated block store location
   */
  protected BlockStoreLocation updateBlockStoreLocation(long bytesToBeAvailable,
      BlockStoreLocation location) {
    return location;
  }
}
