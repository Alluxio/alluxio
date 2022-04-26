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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.AbstractBlockStoreEventListener;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides the basic implementation for every evictor.
 *
 * @deprecated use block annotator instead
 */
@NotThreadSafe
@Deprecated
public abstract class AbstractEvictor extends AbstractBlockStoreEventListener implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractEvictor.class);
  protected final Allocator mAllocator;
  protected BlockMetadataEvictorView mMetadataView;

  /**
   * Creates a new instance of {@link AbstractEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public AbstractEvictor(BlockMetadataEvictorView view, Allocator allocator) {
    mMetadataView = Preconditions.checkNotNull(view, "view");
    mAllocator = Preconditions.checkNotNull(allocator, "allocator");
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
   * {@link #freeSpaceWithView(long, BlockStoreLocation, BlockMetadataEvictorView)}.
   *
   * @param bytesToBeAvailable bytes to be available after eviction
   * @param location target location to evict blocks from
   * @param plan the plan to be recursively updated, is empty when first called in
   *        {@link #freeSpaceWithView(long, BlockStoreLocation, BlockMetadataEvictorView)}
   * @param mode the eviction mode
   * @return the first {@link StorageDirEvictorView} in the range of location
   *         to evict/move bytes from, or null if there is no plan
   */
  protected StorageDirEvictorView cascadingEvict(long bytesToBeAvailable,
      BlockStoreLocation location, EvictionPlan plan, Mode mode) {
    location = updateBlockStoreLocation(bytesToBeAvailable, location);

    // 1. If bytesToBeAvailable can already be satisfied without eviction, return the eligible
    // StorageDirView
    StorageDirEvictorView candidateDirView = selectDirWithRequestedSpace(
        bytesToBeAvailable, location, mMetadataView);
    if (candidateDirView != null) {
      return candidateDirView;
    }

    // 2. Iterate over blocks in order until we find a StorageDirEvictorView that is
    // in the range of location and can satisfy bytesToBeAvailable
    // after evicting its blocks iterated so far
    EvictionDirCandidates dirCandidates = new EvictionDirCandidates();
    Iterator<Long> it = getBlockIterator();
    while (it.hasNext() && dirCandidates.candidateSize() < bytesToBeAvailable) {
      long blockId = it.next();
      try {
        BlockMeta block = mMetadataView.getBlockMeta(blockId);
        if (block != null) { // might not present in this view
          if (block.getBlockLocation().belongsTo(location)) {
            String tierAlias = block.getParentDir().getParentTier().getTierAlias();
            int dirIndex = block.getParentDir().getDirIndex();
            StorageDirView dirView = mMetadataView.getTierView(tierAlias).getDirView(dirIndex);
            if (dirView != null) {
              dirCandidates.add((StorageDirEvictorView) dirView, blockId, block.getBlockSize());
            }
          }
        }
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Remove block {} from evictor cache because {}", blockId, e);
        it.remove();
        onRemoveBlockFromIterator(blockId);
      }
    }

    // 3. If there is no eligible StorageDirEvictorView, return null
    if (mode == Mode.GUARANTEED && dirCandidates.candidateSize() < bytesToBeAvailable) {
      return null;
    }

    // 4. cascading eviction: try to allocate space in the next tier to move candidate blocks
    // there. If allocation fails, the next tier will continue to evict its blocks to free space.
    // Blocks are only evicted from the last tier or it can not be moved to the next tier.
    candidateDirView = dirCandidates.candidateDir();
    if (candidateDirView == null) {
      return null;
    }
    List<Long> candidateBlocks = dirCandidates.candidateBlocks();
    StorageTierView nextTierView
        = mMetadataView.getNextTier(candidateDirView.getParentTierView());
    if (nextTierView == null) {
      // This is the last tier, evict all the blocks.
      for (Long blockId : candidateBlocks) {
        try {
          BlockMeta block = mMetadataView.getBlockMeta(blockId);
          if (block != null) {
            candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
            plan.toEvict().add(new Pair<>(blockId, candidateDirView.toBlockStoreLocation()));
          }
        } catch (BlockDoesNotExistException e) {
          continue;
        }
      }
    } else {
      for (Long blockId : candidateBlocks) {
        try {
          BlockMeta block = mMetadataView.getBlockMeta(blockId);
          if (block == null) {
            continue;
          }
          StorageDirEvictorView nextDirView
              = (StorageDirEvictorView) mAllocator.allocateBlockWithView(
              block.getBlockSize(),
                  BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()),
                  mMetadataView, true);
          if (nextDirView == null) {
            nextDirView = cascadingEvict(block.getBlockSize(),
                BlockStoreLocation.anyDirInTier(nextTierView.getTierViewAlias()), plan, mode);
          }
          if (nextDirView == null) {
            // If we failed to find a dir in the next tier to move this block, evict it and
            // continue. Normally this should not happen.
            plan.toEvict().add(new Pair<>(blockId, block.getBlockLocation()));
            candidateDirView.markBlockMoveOut(blockId, block.getBlockSize());
            continue;
          }
          plan.toMove().add(BlockTransferInfo.createMove(block.getBlockLocation(), blockId,
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
      BlockMetadataEvictorView view) {
    return freeSpaceWithView(bytesToBeAvailable, location, view, Mode.GUARANTEED);
  }

  @Override
  public EvictionPlan freeSpaceWithView(long bytesToBeAvailable, BlockStoreLocation location,
      BlockMetadataEvictorView view, Mode mode) {
    mMetadataView = view;

    List<BlockTransferInfo> toMove = new ArrayList<>();
    List<Pair<Long, BlockStoreLocation>> toEvict = new ArrayList<>();
    EvictionPlan plan = new EvictionPlan(toMove, toEvict);
    StorageDirEvictorView candidateDir = cascadingEvict(bytesToBeAvailable, location, plan, mode);

    mMetadataView.clearBlockMarks();
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
   * Updates the block store location if the evictor wants to free space in a specific location.
   *
   * @param bytesToBeAvailable bytes to be available after eviction
   * @param location the original block store location
   * @return the updated block store location
   */
  protected BlockStoreLocation updateBlockStoreLocation(long bytesToBeAvailable,
      BlockStoreLocation location) {
    return location;
  }

  /**
   * Finds a directory in the given location range with capacity upwards of the given bound.
   *
   * @param bytesToBeAvailable the capacity bound
   * @param location the location range
   * @param mManagerView the storage manager view
   * @return a {@link StorageDirEvictorView} in the range of location that already
   *         has availableBytes larger than bytesToBeAvailable, otherwise null
   */
  @Nullable
  private static StorageDirEvictorView selectDirWithRequestedSpace(long bytesToBeAvailable,
      BlockStoreLocation location, BlockMetadataEvictorView mManagerView) {
    if (location.hasNoRestriction()) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
            return (StorageDirEvictorView) dirView;
          }
        }
      }
      return null;
    }

    String tierAlias = location.tierAlias();
    StorageTierView tierView = mManagerView.getTierView(tierAlias);
    if (location.isAnyDir() && location.isAnyMedium()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
          return (StorageDirEvictorView) dirView;
        }
      }
      return null;
    }

    StorageDirView dirView = tierView.getDirView(location.dir());
    return (dirView != null && dirView.getAvailableBytes() >= bytesToBeAvailable)
        ? (StorageDirEvictorView) dirView : null;
  }
}
