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
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * A greedy allocator that returns the first Storage dir fitting the size of block to allocate.
 * This class serves as an example how to implement an allocator.
 */
public class GreedyAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  public GreedyAllocator(BlockMetadataManagerView view) {
    mManagerView = view;
  }

  @Override
  public TempBlockMeta allocateBlockWithView(final long userId, final long blockId,
      final long blockSize, final BlockStoreLocation location,
      final BlockMetadataManagerView view) {
    mManagerView = view;

    BlockMetadataManagerView.DirVisitor visitor = new BlockMetadataManagerView.DirVisitor() {
      private StorageDirView mDir = null;

      @Override
      public boolean visit(StorageDirView dirView) {
        if (dirView.getAvailableBytes() >= blockSize) {
          mDir = dirView;
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
