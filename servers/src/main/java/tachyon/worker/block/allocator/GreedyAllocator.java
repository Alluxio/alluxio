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

package tachyon.worker.block.allocator;

import java.io.IOException;

import com.google.common.base.Preconditions;

import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * A greedy allocator that returns the first Storage dir fitting the size of block to allocate.
 * This class serves as an example how to implement an allocator.
 */
public class GreedyAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  public GreedyAllocator(BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view);
  }

  @Override
  public TempBlockMeta allocateBlockWithView(long userId, long blockId, long blockSize,
      BlockStoreLocation location, BlockMetadataManagerView view) throws IOException {
    mManagerView = view;
    return allocateBlock(userId, blockId, blockSize, location);
  }

  /**
   * Should only be accessed by {@link allocateBlockWithView} inside class.
   * Allocates a block from the given block store location. The location can be a specific location,
   * or {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(int)}.
   *
   * @param userId the ID of user to apply for the block allocation
   * @param blockId the ID of the block
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @return a temp block meta if success, null otherwise
   * @throws IOException if block location is invalid
   */
  private TempBlockMeta allocateBlock(long userId, long blockId, long blockSize,
      BlockStoreLocation location) throws IOException {
    if (location.equals(BlockStoreLocation.anyTier())) {
      // When any tier is ok, loop over all tier views and dir views,
      // and return a temp block meta from the first available dirview.
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= blockSize) {
            return dirView.createTempBlockMeta(userId, blockId, blockSize);
          }
        }
      }
      return null;
    }

    int tierAlias = location.tierAlias();
    StorageTierView tierView = mManagerView.getTierView(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      // Loop over all dir views in the given tier
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= blockSize) {
          return dirView.createTempBlockMeta(userId, blockId, blockSize);
        }
      }
      return null;
    }

    int dirIndex = location.dir();
    StorageDirView dirView = tierView.getDirView(dirIndex);
    if (dirView.getAvailableBytes() >= blockSize) {
      return dirView.createTempBlockMeta(userId, blockId, blockSize);
    }
    return null;
  }
}
