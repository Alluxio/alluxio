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

package tachyon.worker.block.meta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;

/**
 * This class is a wrapper of {@link StorageDir} to provide more limited access and a filtered list
 * of blocks.
 */
public final class StorageDirView {

  /** the {@link StorageDir} this view is derived from */
  private final StorageDir mDir;
  /** the {@link StorageTierView} this view under */
  private final StorageTierView mTierView;
  /** the {@link BlockMetadataManagerView} this view is associated with */
  private final BlockMetadataManagerView mManagerView;

  // The below data structures are used by the evictor to mark blocks to move in/out during
  // generating an eviction plan.
  private final Set<Long> mBlocksToMoveIn = new HashSet<Long>();
  private final Set<Long> mBlocksToMoveOut = new HashSet<Long>();
  private long mBlocksToMoveInSize = 0L;
  private long mBlocksToMoveOutSize = 0L;

  /**
   * Creates a {@link StorageDirView} using the actual {@link StorageDir} and the associated
   * {@link BlockMetadataManagerView}.
   *
   * @param dir which the dirView is constructed from
   * @param tierView which the dirView is under
   * @param managerView which the dirView is associated with
   */
  public StorageDirView(StorageDir dir, StorageTierView tierView,
      BlockMetadataManagerView managerView) {
    mDir = Preconditions.checkNotNull(dir);
    mTierView = Preconditions.checkNotNull(tierView);
    mManagerView = Preconditions.checkNotNull(managerView);
  }

  /**
   * Gets the index of this Dir.
   *
   * @return index of the dir
   */
  public int getDirViewIndex() {
    return mDir.getDirIndex();
  }

  /**
   * Get a filtered list of block metadata, for blocks that are neither pinned or being blocked.
   *
   * @return a list of metadata for all evictable blocks
   */
  public List<BlockMeta> getEvictableBlocks() {
    List<BlockMeta> filteredList = new ArrayList<BlockMeta>();

    for (BlockMeta blockMeta : mDir.getBlocks()) {
      long blockId = blockMeta.getBlockId();
      if (mManagerView.isBlockEvictable(blockId)) {
        filteredList.add(blockMeta);
      }
    }
    return filteredList;
  }

  /**
   * Gets capacity bytes for this dir.
   *
   * @return capacity bytes for this dir
   */
  public long getCapacityBytes() {
    return mDir.getCapacityBytes();
  }

  /**
   * Gets available bytes for this dir.
   *
   * @return available bytes for this dir
   */
  public long getAvailableBytes() {
    return mDir.getAvailableBytes() + mBlocksToMoveOutSize - mBlocksToMoveInSize;
  }

  /**
   * Gets committed bytes for this dir. This includes all blocks, locked, pinned, committed etc.
   *
   * @return committed bytes for this dir
   */
  public long getCommittedBytes() {
    return mDir.getCommittedBytes();
  }

  /**
   * Gets evictable bytes for this dir, i.e., the total bytes of total evictable blocks
   *
   * @return evictable bytes for this dir
   */
  public long getEvitableBytes() {
    long bytes = 0;
    for (BlockMeta blockMeta : mDir.getBlocks()) {
      long blockId = blockMeta.getBlockId();
      if (mManagerView.isBlockEvictable(blockId)) {
        bytes += blockMeta.getBlockSize();
      }
    }
    return bytes;
  }

  /**
   * Clears all marks about blocks to move in/out in this view.
   */
  public void clearBlockMarks() {
    mBlocksToMoveIn.clear();
    mBlocksToMoveOut.clear();
    mBlocksToMoveInSize = mBlocksToMoveOutSize = 0L;
  }

  /**
   * Creates a {@link TempBlockMeta} given sessionId, blockId, and initialBlockSize.
   *
   * @param sessionId of the owning session
   * @param blockId of the new block
   * @param initialBlockSize of the new block
   * @return a new {@link TempBlockMeta} under the underlying directory
   */
  public TempBlockMeta createTempBlockMeta(long sessionId, long blockId, long initialBlockSize) {
    return new TempBlockMeta(sessionId, blockId, initialBlockSize, mDir);
  }

  /**
   * @return parent tier view
   */
  public StorageTierView getParentTierView() {
    return mTierView;
  }

  /**
   * Returns an indication whether the given block is marked to be moved out.
   *
   * @param blockId the block id
   * @return whether the block is marked to be moved out
   */
  public boolean isMarkedToMoveOut(long blockId) {
    return mBlocksToMoveOut.contains(blockId);
  }

  /**
   * Marks a block to move into this dir view, which is used by the evictor.
   *
   * @param blockId the Id of the block
   * @param blockSize the block size
   */
  public void markBlockMoveIn(long blockId, long blockSize) {
    if (mBlocksToMoveIn.add(blockId)) {
      mBlocksToMoveInSize += blockSize;
    }
  }

  /**
   * Marks a block to move out of this dir view, which is used by the evictor.
   *
   * @param blockId the Id of the block
   * @param blockSize the block size
   */
  public void markBlockMoveOut(long blockId, long blockSize) {
    if (mBlocksToMoveOut.add(blockId)) {
      mBlocksToMoveOutSize += blockSize;
    }
  }

  /**
   * Creates a {@link BlockStoreLocation} for this directory view. Redirecting to
   * {@link StorageDir#toBlockStoreLocation}
   *
   * @return {@link BlockStoreLocation} created
   */
  public BlockStoreLocation toBlockStoreLocation() {
    return mDir.toBlockStoreLocation();
  }
}
