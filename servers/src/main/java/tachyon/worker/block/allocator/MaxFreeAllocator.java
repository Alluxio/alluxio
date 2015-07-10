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
 * An allocator that allocates a block in the storage dir view with most free space.
 */
public class MaxFreeAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  public MaxFreeAllocator() {
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

    StorageDirView candidateDirView = null;
    long maxFreeBytes = blockSize;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= maxFreeBytes) {
            maxFreeBytes = dirView.getAvailableBytes();
            candidateDirView = dirView;
          }
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= maxFreeBytes) {
          maxFreeBytes = dirView.getAvailableBytes();
          candidateDirView = dirView;
        }
      }
    } else {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      StorageDirView dirView = tierView.getDirView(location.dir());
      if (dirView.getAvailableBytes() >= blockSize) {
        candidateDirView = dirView;
      }
    }

    // TODO: avoid getDirForCreatingBlock()
    return candidateDirView != null
        ? new TempBlockMeta(userId, blockId, blockSize, candidateDirView.getDirForCreatingBlock())
        : null;
  }
}
