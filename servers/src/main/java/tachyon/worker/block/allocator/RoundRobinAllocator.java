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
import java.util.HashMap;
import java.util.Map;

import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * A round-robin allocator that allocates a block in the storage dir. It will allocate the block in
 * the highest tier possible: It always starts from the highest tier in a RR manner and goes to the
 * next tier when there is no enough space. The allocator only considers non-specific writes in its
 * RR policy (the location is either AnyTier or AnyDirInTier).
 */
public class RoundRobinAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  // We need to remember the last dir index for every storage tier
  private final Map<StorageTierView, Integer> mTierToLastDirMap =
      new HashMap<StorageTierView, Integer>();

  public RoundRobinAllocator(BlockMetadataManagerView view) {
    mManagerView = view;
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      mTierToLastDirMap.put(tierView, -1);
    }
  }

  @Override
  public TempBlockMeta allocateBlockWithView(final long userId, final long blockId,
      final long blockSize, final BlockStoreLocation location,
      final BlockMetadataManagerView view) {
    mManagerView = view;

    BlockMetadataManagerView.DirVisitor visitor = new BlockMetadataManagerView.DirVisitor() {
      private int mDirViewIndex;
      private StorageDirView mDir = null;

      @Override
      public boolean visit(StorageDirView dirView) {
        StorageTierView tierView = dirView.getParentTierView();
        mDirViewIndex = mTierToLastDirMap.get(tierView);
        mDirViewIndex = (mDirViewIndex + 1) % tierView.getDirViews().size();
        mTierToLastDirMap.put(tierView, mDirViewIndex);
        StorageDirView dir = tierView.getDirView(mDirViewIndex);
        if (dir.getAvailableBytes() >= blockSize) {
          mDir = dir;
          return true;
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
