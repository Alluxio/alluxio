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

package alluxio.worker.block.evictor;

import alluxio.Constants;
import alluxio.Sessions;
import alluxio.collections.Pair;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreEventListenerBase;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides the basic implementation for every evictor.
 */
@NotThreadSafe
public abstract class EvictorBase extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  protected final Allocator mAllocator;
  protected BlockMetadataManagerView mManagerView;

  /**
   * Creates a new instance of {@link EvictorBase}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public EvictorBase(BlockMetadataManagerView view, Allocator allocator) {
    mManagerView = Preconditions.checkNotNull(view);
    mAllocator = Preconditions.checkNotNull(allocator);
  }

  /**
   * A recursive implementation of cascading eviction.
   *
   * This method uses a specific eviction strategy to find blocks to evict in the requested
   * location. After eviction, one {@link alluxio.worker.block.meta.StorageDir} in the location has
   * the specific amount of free space. It then uses an allocation strategy to allocate space in the
   * next tier to move each evicted blocks. If the next tier fails to allocate space for the evicted
   * blocks, the next tier will continue to evict its blocks to free space.
   *
   * This method is only used in
   * {@link #freeSpaceWithView(long, BlockStoreLocation, BlockMetadataManagerView)}.
   *
   * @param bytesToBeAvailable bytes to be available after eviction
   * @param location target location to evict blocks from
   * @param plan the plan to be recursively updated, is empty when first called in
   *        {@link #freeSpaceWithView(long, BlockStoreLocation, BlockMetadataManagerView)}
   * @return the first {@link StorageDirView} in the range of location to evict/move bytes from, or
   *         null if there is no plan
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
    Iterator<Long> it = getBlockIterator();
    while (it.hasNext() && dirCandidates.candidateSize() < bytesToBeAvailable) {
      long blockId = it.next();
      try {
        BlockMeta block = mManagerView.getBlockMeta(blockId);
        if (block != null) { // might not present in this view
          if (block.getBlockLocation().belongsTo(location)) {
            String tierAlias = block.getParentDir().getParentTier().getTierAlias();
            int dirIndex = block.getParentDir().getDirIndex();
            dirCandidates.add(mManagerView.getTierView(tierAlias).getDirView(dirIndex), blockId,
                block.getBlockSize());
          }
        }
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Remove block {} from evictor cache because {}", blockId, e);
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
      for (Long blockId : candidateBlocks) {
        try {
          BlockMeta block = mManagerView.getBlockMeta(blockId);
          if (block != null) {
            candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
            plan.toEvict().add(new Pair<Long, BlockStoreLocation>(blockId,
                candidateDirView.toBlockStoreLocation()));
          }
        } catch (BlockDoesNotExistException e) {
          continue;
        }
      }
    } else {
      for (Long blockId : candidateBlocks) {
        try {
          BlockMeta block = mManagerView.getBlockMeta(blockId);
          if (block == null) {
            continue;
          }
          StorageDirView nextDirView = mAllocator.allocateBlockWithView(
              Sessions.MIGRATE_DATA_SESSION_ID, block.getBlockSize(),
              BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()), mManagerView);
          if (nextDirView == null) {
            nextDirView = cascadingEvict(block.getBlockSize(),
                BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()), plan);
          }
          if (nextDirView == null) {
            // If we failed to find a dir in the next tier to move this block, evict it and
            // continue. Normally this should not happen.
            plan.toEvict().add(new Pair<Long, BlockStoreLocation>(blockId,
                block.getBlockLocation()));
            candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
            continue;
          }
          plan.toMove().add(new BlockTransferInfo(blockId, block.getBlockLocation(),
              nextDirView.toBlockStoreLocation()));
          candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
          nextDirView.markBlockMoveIn(blockId, block.getBlockSize());
        } catch (BlockDoesNotExistException e) {
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

    List<BlockTransferInfo> toMove = new ArrayList<BlockTransferInfo>();
    List<Pair<Long, BlockStoreLocation>> toEvict = new ArrayList<Pair<Long, BlockStoreLocation>>();
    EvictionPlan plan = new EvictionPlan(toMove, toEvict);
    StorageDirView candidateDir = cascadingEvict(bytesToBeAvailable, location, plan);

    mManagerView.clearBlockMarks();
    if (candidateDir == null) {
      return null;
    }

    return plan;
  }

  /**
   * Returns an iterator for evictor cache blocks. The evictor is responsible for specifying the
   * iteration order using its own strategy. For example, {@link LRUEvictor} returns an iterator
   * that iterates through the block ids in LRU order.
   *
   * @return an iterator over the ids of the blocks in the evictor cache
   */
  protected abstract Iterator<Long> getBlockIterator();

  /**
   * Performs additional cleanup when a block is removed from the iterator returned by
   * {@link #getBlockIterator()}.
   */
  protected void onRemoveBlockFromIterator(long blockId) {}

  /**
   * Updates the block store location if the evictor wants to free space in a specific location. For
   * example, {@link PartialLRUEvictor} always evicts blocks from a dir with max free space.
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
