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

  public GreedyAllocator(BlockMetadataManagerView metadata) {
    mManagerView = Preconditions.checkNotNull(metadata);
  }

  @Override
  public TempBlockMeta allocateBlockWithView(long userId, long blockId, long blockSize,
      BlockStoreLocation location, BlockMetadataManagerView view) throws IOException {
    mManagerView = view;
    return allocateBlock(userId, blockId, blockSize, location);
  }

  @Override
  public TempBlockMeta allocateBlock(long userId, long blockId, long blockSize,
      BlockStoreLocation location) throws IOException {
    if (location.equals(BlockStoreLocation.anyTier())) {
      // When any tierView is ok, loop over all storage tierViews and dirViews,
      // and return the first dirView that has sufficient available space.
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= blockSize) {
            // TODO: have to get underlying dir here, break the design of dirView
            return new TempBlockMeta(userId, blockId, blockSize, dirView.getDirForCreatingBlock());
          }
        }
      }
      return null;
    }

    int tierAlias = location.tierAlias();
    StorageTierView tierView = mManagerView.getTierView(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      // Loop over all dirViewss in the given tierView
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= blockSize) {
          // TODO: have to get underlying dir here, break the design of dirView
          return new TempBlockMeta(userId, blockId, blockSize, dirView.getDirForCreatingBlock());
        }
      }
      return null;
    }

    int dirIndex = location.dir();
    StorageDirView dirView = tierView.getDirView(dirIndex);
    if (dirView.getAvailableBytes() >= blockSize) {
      return new TempBlockMeta(userId, blockId, blockSize, dirView.getDirForCreatingBlock());
    }
    return null;
  }
}
