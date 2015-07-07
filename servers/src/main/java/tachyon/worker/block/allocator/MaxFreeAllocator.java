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

import tachyon.worker.block.BlockMetadataView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * An allocator that allocates a block in the storage dir with most free space.
 */
public class MaxFreeAllocator implements Allocator {
  private BlockMetadataView mMetaView;

  public MaxFreeAllocator(BlockMetadataView metadata) {
    mMetaView = Preconditions.checkNotNull(metadata);
  }

  @Override
  public TempBlockMeta allocateBlockWithView(long userId, long blockId, long blockSize,
      BlockStoreLocation location, BlockMetadataView view) throws IOException {
    mMetaView = view;
    return allocateBlock(userId, blockId, blockSize, location);
  }

  @Override
  public TempBlockMeta allocateBlock(long userId, long blockId, long blockSize,
      BlockStoreLocation location) throws IOException {

    StorageDirView candidateDir = null;
    long maxFreeBytes = blockSize;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tier : mMetaView.getTierViews()) {
        for (StorageDirView dir : tier.getDirViews()) {
          if (dir.getAvailableBytes() >= maxFreeBytes) {
            maxFreeBytes = dir.getAvailableBytes();
            candidateDir = dir;
          }
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tier = mMetaView.getTierView(location.tierAlias());
      for (StorageDirView dir : tier.getDirViews()) {
        if (dir.getAvailableBytes() >= maxFreeBytes) {
          maxFreeBytes = dir.getAvailableBytes();
          candidateDir = dir;
        }
      }
    } else {
      StorageTierView tier = mMetaView.getTierView(location.tierAlias());
      StorageDirView dir = tier.getDirView(location.dir());
      if (dir.getAvailableBytes() >= blockSize) {
        candidateDir = dir;
      }
    }

    // TODO: avoid getDirForCreatingBlock()
    return candidateDir != null
        ? new TempBlockMeta(userId, blockId, blockSize, candidateDir.getDirForCreatingBlock())
        : null;
  }
}
