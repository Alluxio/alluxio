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
  private Map<StorageTierView, Integer> mTierToLastDirMap = new HashMap<StorageTierView, Integer>();

  public RoundRobinAllocator(BlockMetadataManagerView view) {
    mManagerView = view;
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      mTierToLastDirMap.put(tierView, -1);
    }
  }

  @Override
  public StorageDirView allocateBlockWithView(long userId, long blockSize,
      BlockStoreLocation location, BlockMetadataManagerView view) {
    mManagerView = view;
    return allocateBlock(userId, blockSize, location);
  }

  /**
   * Should only be accessed by {@link allocateBlockWithView} inside class. Allocates a block from
   * the given block store location. The location can be a specific location, or
   * {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(int)}.
   *
   * @param userId the ID of user to apply for the block allocation
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @return a StorageDirView in which to create the temp block meta if success, null otherwise
   * @throws IllegalArgumentException if block location is invalid
   */
  private StorageDirView allocateBlock(long userId, long blockSize, BlockStoreLocation location) {
    if (location.equals(BlockStoreLocation.anyTier())) {
      int tierIndex = 0; // always starting from the first tier
      for (int i = 0; i < mManagerView.getTierViews().size(); i ++) {
        StorageTierView tierView = mManagerView.getTierViews().get(tierIndex);
        int dirViewIndex = getNextAvailDirInTier(tierView, blockSize);
        if (dirViewIndex >= 0) {
          mTierToLastDirMap.put(tierView, dirViewIndex); // update
          return tierView.getDirView(dirViewIndex);
        } else { // we didn't find one in this tier, go to next tier
          tierIndex ++;
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      int dirViewIndex = getNextAvailDirInTier(tierView, blockSize);
      if (dirViewIndex >= 0) {
        mTierToLastDirMap.put(tierView, dirViewIndex); // update
        return tierView.getDirView(dirViewIndex);
      }
    } else {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      StorageDirView dirView = tierView.getDirView(location.dir());
      if (dirView.getAvailableBytes() >= blockSize) {
        return dirView;
      }
    }

    return null;
  }

  /**
   * Find an available dir in a given tier for a block with blockSize
   *
   * @param tier: the tier to find a dir
   * @param blockSize: the requested block size
   * @return: the index of the dir if nonnegative; -1 if fail to find a dir
   */
  private int getNextAvailDirInTier(StorageTierView tierView, long blockSize) {
    int dirViewIndex = mTierToLastDirMap.get(tierView);
    for (int i = 0; i < tierView.getDirViews().size(); i ++) { // try this many times
      dirViewIndex = (dirViewIndex + 1) % tierView.getDirViews().size();
      if (tierView.getDirView(dirViewIndex).getAvailableBytes() >= blockSize) {
        return dirViewIndex;
      }
    }
    return -1;
  }
}
