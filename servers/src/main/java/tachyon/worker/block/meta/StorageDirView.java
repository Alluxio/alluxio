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
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.worker.block.BlockMetadataManagerView;

/**
 * This class is a wrapper of {@link StorageDir} to provide more limited access
 * and a filtered list of blocks.
 */
public class StorageDirView {

  /** the StorageDir this view is derived from */
  private final StorageDir mDir;
  /** the StorageTierView this view under */
  private final StorageTierView mTierView;
  /** the BlockMetadataView this view is associated with */
  private final BlockMetadataManagerView mManagerView;

  /**
   * Create a StorageDirView using the actual StorageDir and the associated BlockMetadataView
   *
   * @param dir which the dirView is constructed from
   * @param tierView which the dirView is under
   * @param managerView which the dirView is associated with
   * @return StorageDirView constructed
   */
  public StorageDirView(StorageDir dir, StorageTierView tierView,
      BlockMetadataManagerView managerView) {
    mDir = Preconditions.checkNotNull(dir);
    mTierView = Preconditions.checkNotNull(tierView);
    mManagerView = Preconditions.checkNotNull(managerView);
  }

  /**
   * Get the index of this Dir
   *
   * @return index of the dir
   */
  public int getDirViewIndex() {
    return mDir.getDirIndex();
  }

  /**
   * Get a filtered list of block metadata,
   * for blocks that are neither pinned or being blocked.
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
   * Get available bytes for this dir
   *
   * @return available bytes for this dir
   */
  public long getAvailableBytes() {
    return mDir.getAvailableBytes();
  }

  /**
   * Get committed bytes for this dir.
   * This includes all blocks, locked, pinned, committed etc.
   *
   * @return committed bytes for this dir
   */
  public long getCommittedBytes() {
    return mDir.getCommittedBytes();
  }

  /**
   * Get evictable bytes for this dir, i.e., the total bytes of total evictable blocks
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
   * Create a TempBlockMeta given userId, blockId, and initialBlockSize.
   *
   * @param userId of the owning user
   * @param blockId of the new block
   * @param initialBlockSize of the new block
   * @return a new TempBlockMeta under the underlying directory.
   */
  public TempBlockMeta createTempBlockMeta(long userId, long blockId, long initialBlockSize) {
    return new TempBlockMeta(userId, blockId, initialBlockSize, mDir);
  }

  /**
   * Get the parent TierView for this view.
   *
   * @return parent tierview
   */
  public StorageTierView getParentTierView() {
    return mTierView;
  }
}
