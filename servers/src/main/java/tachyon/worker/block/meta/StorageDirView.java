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
import java.util.Set;

import com.google.common.base.Preconditions;

import tachyon.master.BlockInfo;
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
   */
  public StorageDirView(StorageDir dir, StorageTierView tier, BlockMetadataManagerView view) {
    mDir = Preconditions.checkNotNull(dir);
    mTierView = Preconditions.checkNotNull(tier);
    mManagerView = Preconditions.checkNotNull(view);
  }

  /**
   * Get the index of this Dir
   */
  public int getDirIndex() {
    return mDir.getDirIndex();
  }

  /**
   * Return a filtered list of block metadata,
   * for blocks that are neither pinned or being blocked.
   */
  public List<BlockMeta> getEvictableBlocks() {
    List<BlockMeta> filteredList = new ArrayList<BlockMeta>();

    for (BlockMeta blockMeta : mDir.getBlocks()) {
      long blockId = blockMeta.getBlockId();
      if (!mManagerView.isBlockPinned(blockId) && !mManagerView.isBlockLocked(blockId)) {
        filteredList.add(blockMeta);
      }
    }
    return filteredList;
  }

  /**
   * Get available bytes for this dir
   */
  public long getAvailableBytes() {
    return mDir.getAvailableBytes();
  }

  /**
   * Get committed bytes for this dir
   */
  public long getCommittedBytes() {
    // TODO: does pinedList, etc. change this value?
    return mDir.getCommittedBytes();
  }

  /**
   * Get the actual dir for this view.
   * This API is here so that to be compatible with Allocator implementation.
   * Ideally it should be removed.
   */
  public StorageDir getDirForCreatingBlock() {
    return mDir;
  }

  /**
   * Get the parent TierView for this view.
   */
  public StorageTierView getParentTierView() {
    return mTierView;
  }
}
