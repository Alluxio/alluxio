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

package alluxio.worker.block;

import alluxio.exception.BlockDoesNotExistException;
import alluxio.master.block.BlockId;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.StorageTierEvictorView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class exposes a narrower view of {@link BlockMetadataManager} to Evictors,
 * filtering out un-evictable blocks and un-allocatable space internally, so that evictors and
 * allocators can be developed with much simpler logic, without worrying about various constraints,
 * e.g. pinned files, locked blocks, etc.
 *
 * TODO(cc): Filter un-allocatable space.
 */
@NotThreadSafe
public class BlockMetadataEvictorView extends BlockMetadataView {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMetadataEvictorView.class);

  /** A list of pinned inodes, including inodes which are scheduled for async persist. */
  private final Set<Long> mPinnedInodes = new HashSet<>();

  /** Indices of locks that are being used. */
  private final Set<Long> mInUseBlocks = new HashSet<>();

  /**
   * Creates a new instance of {@link BlockMetadataEvictorView}. Now we always create a new view
   * before freespace.
   *
   * @param manager which the view should be constructed from
   * @param pinnedInodes a set of pinned inodes
   * @param lockedBlocks a set of locked blocks
   */
  // TODO(qifan): Incrementally update the view.
  public BlockMetadataEvictorView(BlockMetadataManager manager, Set<Long> pinnedInodes,
      Set<Long> lockedBlocks) {
    super(manager);
    mPinnedInodes.addAll(Preconditions.checkNotNull(pinnedInodes, "pinnedInodes"));
    Preconditions.checkNotNull(lockedBlocks, "lockedBlocks");
    mInUseBlocks.addAll(lockedBlocks);
  }

  @Override
  protected void initializeView() {
    // iteratively create all StorageTierViews and StorageDirViews
    for (StorageTier tier : mMetadataManager.getTiers()) {
      StorageTierEvictorView tierView = new StorageTierEvictorView(tier, this);
      mTierViews.add(tierView);
      mAliasToTierViews.put(tier.getTierAlias(), tierView);
    }
  }

  /**
   * Gets all dir views that belongs to a given location.
   *
   * @param location the location
   * @return the list of dir views
   */
  public List<StorageDirView> getDirs(BlockStoreLocation location) {
    List<StorageDirView> dirs = new LinkedList<>();
    for (StorageTierView tier : mTierViews) {
      for (StorageDirView dir : tier.getDirViews()) {
        if (dir.toBlockStoreLocation().belongsTo(location)) {
          dirs.add(dir);
        }
      }
    }
    return dirs;
  }

  /**
   * Clears all marks of blocks to move in/out in all dir views.
   */
  public void clearBlockMarks() {
    for (StorageTierView tierView : mTierViews) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        ((StorageDirEvictorView) dirView).clearBlockMarks();
      }
    }
  }

  /**
   * Tests if the block is pinned either explicitly or because it is scheduled for async persist.
   *
   * @param blockId to be tested
   * @return boolean, true if block is pinned
   */
  public boolean isBlockPinned(long blockId) {
    return mPinnedInodes.contains(BlockId.getFileId(blockId));
  }

  /**
   * Tests if the block is locked.
   *
   * @param blockId to be tested
   * @return boolean, true if block is locked
   */
  public boolean isBlockLocked(long blockId) {
    return mInUseBlocks.contains(blockId);
  }

  /**
   * Tests if the block is evictable.
   *
   * @param blockId to be tested
   * @return boolean, true if the block can be evicted
   */
  public boolean isBlockEvictable(long blockId) {
    boolean pinned = isBlockPinned(blockId);
    boolean locked = isBlockLocked(blockId);
    boolean marked = isBlockMarked(blockId);
    boolean isEvictable = !pinned && !locked && !marked;
    if (!isEvictable) {
      LOG.debug("Block not evictable: {}. Pinned: {}, Locked: {}, Marked: {}", blockId, pinned,
          locked, marked);
    }

    return isEvictable;
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
        if (((StorageDirEvictorView) dirView).isMarkedToMoveOut(blockId)) {
          return true;
        }
      }
    }
    return false;
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
   * @throws BlockDoesNotExistException if no {@link BlockMeta} for this block id is found
   */
  @Nullable
  public BlockMeta getBlockMeta(long blockId) throws BlockDoesNotExistException {
    if (isBlockEvictable(blockId)) {
      return mMetadataManager.getBlockMeta(blockId);
    } else {
      return null;
    }
  }
}
