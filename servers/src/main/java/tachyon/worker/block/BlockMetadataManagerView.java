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

package tachyon.worker.block;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import tachyon.exception.ExceptionMessage;
import tachyon.exception.NotFoundException;
import tachyon.master.block.BlockId;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.StorageTierView;

/**
 * This class exposes a narrower view of {@link BlockMetadataManager} to Evictors and Allocators,
 * filtering out un-evictable blocks and un-allocatable space (TODO) internally, so that evictors
 * and allocators can be developed with much simpler logic, without worrying about various
 * constraints, e.g. pinned files, locked blocks, etc.
 */
public class BlockMetadataManagerView {

  /** The BlockMetadataManager this view is derived from */
  private final BlockMetadataManager mMetadataManager;
  /** A list of StorageTierView, derived from StorageTiers from the BlockMetadataManager */
  private List<StorageTierView> mTierViews = new ArrayList<StorageTierView>();
  /** A list of pinned inodes */
  private final Set<Long> mPinnedInodes = new HashSet<Long>();
  /** Indices of locks that are being used */
  private final BitSet mInUseLocks = new BitSet();
  /** A map from tier level to StorageTierView */
  private Map<Integer, StorageTierView> mLevelToTierViews = new HashMap<Integer, StorageTierView>();

  /**
   * Constructor of BlockMatadataManagerView. Now we always creating a new view before freespace.
   * TODO: incrementally update the view
   *
   * @param manager which the view should be constructed from
   * @param pinnedInodes a set of pinned inodes
   * @param lockedBlocks a set of locked blocks
   */
  public BlockMetadataManagerView(BlockMetadataManager manager, Set<Long> pinnedInodes,
      Set<Long> lockedBlocks) {
    mMetadataManager = Preconditions.checkNotNull(manager);
    mPinnedInodes.addAll(Preconditions.checkNotNull(pinnedInodes));
    Preconditions.checkNotNull(lockedBlocks);
    for (Long blockId : lockedBlocks) {
      mInUseLocks.set(BlockLockManager.blockHashIndex(blockId));
    }

    // iteratively create all StorageTierViews and StorageDirViews
    for (StorageTier tier : manager.getTiers()) {
      StorageTierView tierView = new StorageTierView(tier, this);
      mTierViews.add(tierView);
      mLevelToTierViews.put(tier.getTierLevel(), tierView);
    }
  }

  /**
   * Clears all marks of blocks to move in/out in all dir views.
   */
  public void clearBlockMarks() {
    for (StorageTierView tierView : mTierViews) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        dirView.clearBlockMarks();
      }
    }
  }

  /**
   * Tests if the block is pinned.
   *
   * @param blockId to be tested
   * @return boolean, true if block is pinned
   */
  public boolean isBlockPinned(long blockId) {
    return mPinnedInodes.contains(
        BlockId.createBlockId(BlockId.getContainerId(blockId), BlockId.getMaxSequenceNumber()));
  }

  /**
   * Tests if the block is locked.
   *
   * @param blockId to be tested
   * @return boolean, true if block is locked
   */
  public boolean isBlockLocked(long blockId) {
    int index = BlockLockManager.blockHashIndex(blockId);
    if (index < mInUseLocks.length()) {
      return mInUseLocks.get(index);
    } else {
      return false;
    }
  }

  /**
   * Tests if the block is evictable.
   *
   * @param blockId to be tested
   * @return boolean, true if the block can be evicted
   */
  public boolean isBlockEvictable(long blockId) {
    return (!isBlockPinned(blockId) && !isBlockLocked(blockId) && !isBlockMarked(blockId));
  }

  /**
   * Test if the block is marked to move out of its current dir in this view.
   *
   * @param blockId the Id of the block
   * @return boolean, true if the block is marked to move out
   */
  public boolean isBlockMarked(long blockId) {
    for (StorageTierView tierView : mTierViews) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.isMarkedToMoveOut(blockId)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Provides StorageTierView given tierLevel.
   *
   * @param tierLevel the level of this tierView
   * @return the StorageTierView object associated with the level
   * @throws IllegalArgumentException if tierLevel is not found
   */
  public StorageTierView getTierView(int tierLevel) {
    StorageTierView tierView = mLevelToTierViews.get(tierLevel);
    if (null == tierView) {
      throw new IllegalArgumentException(
          ExceptionMessage.TIER_VIEW_ALIAS_NOT_FOUND.getMessage(tierLevel));
    } else {
      return tierView;
    }
  }

  /**
   * Gets all tierViews under this managerView.
   *
   * @return the list of StorageTierViews
   */
  public List<StorageTierView> getTierViews() {
    return mTierViews;
  }

  /**
   * Gets all tierViews before certain tierView.
   *
   * @param tierLevel the level of a tierView
   * @return the list of StorageTierView
   * @throws IllegalArgumentException if tierLevel is not found
   */
  public List<StorageTierView> getTierViewsBelow(int tierLevel) {
    int level = getTierView(tierLevel).getTierViewLevel();
    return mTierViews.subList(level + 1, mTierViews.size());
  }

  /**
   * Get the next storage tier view.
   *
   * @param tierView the storage tier view
   * @return the next storage tier view, null if this is the last tier view.
   */
  public StorageTierView getNextTier(StorageTierView tierView) {
    int nextLevel = tierView.getTierViewLevel() + 1;
    if (nextLevel < mTierViews.size()) {
      return mTierViews.get(nextLevel);
    }
    return null;
  }

  /**
   * Get available bytes given certain location
   * {@link BlockMetadataManager#getAvailableBytes(BlockStoreLocation)}.
   *
   * @param location location the check available bytes
   * @return available bytes
   * @throws IllegalArgumentException if location does not belong to tiered storage
   */
  public long getAvailableBytes(BlockStoreLocation location) {
    return mMetadataManager.getAvailableBytes(location);
  }

  /**
   * Returns null if block is pinned or currently being locked, otherwise returns
   * {@link BlockMetadataManager#getBlockMeta(long)}.
   *
   * @param blockId the block ID
   * @return metadata of the block or null
   * @throws NotFoundException if no BlockMeta for this blockId is found
   */
  public BlockMeta getBlockMeta(long blockId) throws NotFoundException {
    if (isBlockEvictable(blockId)) {
      return mMetadataManager.getBlockMeta(blockId);
    } else {
      return null;
    }
  }
}
