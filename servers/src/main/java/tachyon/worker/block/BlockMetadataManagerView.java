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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.ExceptionMessage;
import tachyon.master.block.BlockId;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.StorageTierView;

/**
 * This class exposes a narrower view of {@link BlockMetadataManager} to Evictors and Allocators,
 * filtering out un-evictable blocks and un-allocatable space internally, so that evictors and
 * allocators can be developed with much simpler logic, without worrying about various constraints,
 * e.g. pinned files, locked blocks, etc.
 *
 * TODO(cc): Filter un-allocatable space.
 */
public class BlockMetadataManagerView {

  /** The {@link BlockMetadataManager} this view is derived from */
  private final BlockMetadataManager mMetadataManager;
  /**
   * A list of {@link StorageTierView}, derived from {@link StorageTier}s from the
   * {@link BlockMetadataManager}
   */
  private List<StorageTierView> mTierViews = new ArrayList<StorageTierView>();
  /** A list of pinned inodes */
  private final Set<Long> mPinnedInodes = new HashSet<Long>();
  /** Indices of locks that are being used */
  private final BitSet mInUseLocks = new BitSet();
  /** A map from tier alias to {@link StorageTierView} */
  private Map<String, StorageTierView> mAliasToTierViews = new HashMap<String, StorageTierView>();

  /**
   * Creates a new instance of {@link BlockMetadataManagerView}. Now we always create a new view
   * before freespace.
   *
   * @param manager which the view should be constructed from
   * @param pinnedInodes a set of pinned inodes
   * @param lockedBlocks a set of locked blocks
   */
  // TODO(qifan): Incrementally update the view.
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
      mAliasToTierViews.put(tier.getTierAlias(), tierView);
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
   * Tests if the block is marked to move out of its current dir in this view.
   *
   * @param blockId the id of the block
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
   * Provides {@link StorageTierView} given tierAlias. Throws an {@link IllegalArgumentException} if
   * the tierAlias is not found.
   *
   * @param tierAlias the alias of this tierView
   * @return the {@link StorageTierView} object associated with the alias
   */
  public StorageTierView getTierView(String tierAlias) {
    StorageTierView tierView = mAliasToTierViews.get(tierAlias);
    if (tierView == null) {
      throw new IllegalArgumentException(
          ExceptionMessage.TIER_VIEW_ALIAS_NOT_FOUND.getMessage(tierAlias));
    } else {
      return tierView;
    }
  }

  /**
   * Gets all tierViews under this managerView.
   *
   * @return the list of {@link StorageTierView}s
   */
  public List<StorageTierView> getTierViews() {
    return Collections.unmodifiableList(mTierViews);
  }

  /**
   * Gets all tierViews before certain tierView. Throws an {@link IllegalArgumentException} if the
   * tierAlias is not found.
   *
   * @param tierAlias the alias of a tierView
   * @return the list of {@link StorageTierView}
   */
  public List<StorageTierView> getTierViewsBelow(String tierAlias) {
    int ordinal = getTierView(tierAlias).getTierViewOrdinal();
    return mTierViews.subList(ordinal + 1, mTierViews.size());
  }

  /**
   * Gets the next storage tier view.
   *
   * @param tierView the storage tier view
   * @return the next storage tier view, null if this is the last tier view
   */
  public StorageTierView getNextTier(StorageTierView tierView) {
    int nextOrdinal = tierView.getTierViewOrdinal() + 1;
    if (nextOrdinal < mTierViews.size()) {
      return mTierViews.get(nextOrdinal);
    }
    return null;
  }

  /**
   * Gets available bytes given certain location
   * {@link BlockMetadataManager#getAvailableBytes(BlockStoreLocation)}. Throws an
   * {@link IllegalArgumentException} if the location does not belong to tiered storage.
   *
   * @param location location the check available bytes
   * @return available bytes
   */
  public long getAvailableBytes(BlockStoreLocation location) {
    return mMetadataManager.getAvailableBytes(location);
  }

  /**
   * Returns null if block is pinned or currently being locked, otherwise returns
   * {@link BlockMetadataManager#getBlockMeta(long)}.
   *
   * @param blockId the block id
   * @return metadata of the block or null
   * @throws BlockDoesNotExistException if no {@link BlockMeta} for this blockId is found
   */
  public BlockMeta getBlockMeta(long blockId) throws BlockDoesNotExistException {
    if (isBlockEvictable(blockId)) {
      return mMetadataManager.getBlockMeta(blockId);
    } else {
      return null;
    }
  }
}
