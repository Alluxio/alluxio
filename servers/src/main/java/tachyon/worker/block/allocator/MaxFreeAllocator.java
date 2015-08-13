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

import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * An allocator that allocates a block in the storage dir with most free space.
 * It always allocates to the highest tier if the requested block store location is any tier.
 */
public class MaxFreeAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  public MaxFreeAllocator(BlockMetadataManagerView view) {
    mManagerView = view;
  }

  @Override
  public TempBlockMeta allocateBlockWithView(final long userId, final long blockId,
      final long blockSize, final BlockStoreLocation location,
      final BlockMetadataManagerView view) {
    mManagerView = view;

    BlockMetadataManagerView.DirVisitor visitor = new BlockMetadataManagerView.DirVisitor() {
      private long mMaxFreeBytes = blockSize - 1;
      private StorageDirView mDir = null;
      private StorageTierView mTier = null; // the highest tier that can allocate the space

      @Override
      public boolean visit(StorageDirView dirView) {
        StorageTierView tier = dirView.getParentTierView();
        if (mTier != null && mTier != tier) {
          // there is already a higher tier to allocate the space, stop the iteration
          return true;
        }
        if (dirView.getAvailableBytes() > mMaxFreeBytes) {
          mMaxFreeBytes = dirView.getAvailableBytes();
          mDir = dirView;
          if (mTier == null) {
            mTier = tier; // the highest tier to allocate the space
          }
        }
        return false;
      }

      @Override
      public StorageDirView getDir() {
        return mDir;
      }
    };

    mManagerView.visitDirs(location, visitor);
    StorageDirView dir = visitor.getDir();
    return dir == null ? null : dir.createTempBlockMeta(userId, blockId, blockSize);
  }
}
