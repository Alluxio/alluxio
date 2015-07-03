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

import com.google.common.base.Preconditions;

import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * A Round robin allocator that allocates a block in the storage dir.
 * It will allocate the block in the highest tier possible:
 * It always starts from the highest tier in a RR manner and goes to the next tier when there
 * is no enough space. 
 * The allocator only considers non-specific writes in its RR policy (the location is either 
 * AnyTier or AnyDirInTier).
 */
public class RRAllocator implements Allocator {
  private final BlockMetadataManager mMetaManager;

  // We need to remember the last dir index for every storage tier
  private Map<StorageTier, Integer> mTierDirs = new HashMap<StorageTier, Integer>();

  public RRAllocator(BlockMetadataManager metadata) {
    mMetaManager = Preconditions.checkNotNull(metadata);
    
    for (StorageTier tier : mMetaManager.getTiers()) {
      mTierDirs.put(tier, -1);
    }
  }

  @Override
  public TempBlockMeta allocateBlock(long userId, long blockId, long blockSize,
      BlockStoreLocation location) throws IOException {

    if (location.equals(BlockStoreLocation.anyTier())) {
      int tierAlias = 0; //always starting from the first tier
      for (int i = 0; i < mMetaManager.getTiers().size(); i ++) {
        StorageTier tier = mMetaManager.getTiers().get(tierAlias);
        int dirIndex = getNextDirInTier(tier, blockSize);
        if (dirIndex >= 0) {
          mTierDirs.put(tier, dirIndex); // update
          return new TempBlockMeta(userId, blockId, blockSize, tier.getDir(dirIndex));
        } else { // we didn't find one in this tier, go to next tier
          tierAlias ++;
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTier tier = mMetaManager.getTier(location.tierAlias());
      int dirIndex = getNextDirInTier(tier, blockSize);
      if (dirIndex >= 0) {
        mTierDirs.put(tier, dirIndex); // update
        return new TempBlockMeta(userId, blockId, blockSize, tier.getDir(dirIndex));
      }
    } else {
      StorageTier tier = mMetaManager.getTier(location.tierAlias());
      StorageDir dir = tier.getDir(location.dir());
      if (dir.getAvailableBytes() >= blockSize) {
        return new TempBlockMeta(userId, blockId, blockSize, dir);
      }
    }

    return null;
  }

  /**
   * Find an available dir in a given tier
   * @param tier
   * @param blockSize
   * @return
   */
  private int getNextDirInTier(StorageTier tier, long blockSize) {
    int dirIndex = mTierDirs.get(tier);
    for (int i = 0; i < tier.getStorageDirs().size(); i ++) { //try this many times
      dirIndex = (dirIndex + 1) % tier.getStorageDirs().size();
      if (tier.getDir(dirIndex).getAvailableBytes() >= blockSize) {
        return dirIndex;
      }
    }
    return -1;
  }
}
