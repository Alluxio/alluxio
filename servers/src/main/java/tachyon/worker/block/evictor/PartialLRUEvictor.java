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

package tachyon.worker.block.evictor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * This class is used to evict old blocks in certain StorageDir by LRU. The main difference
 * between PartialLRU and LRU is that LRU choose old blocks among several StorageDirs
 * until one StorageDir satisfies the request space, but PartialLRU select one StorageDir
 * first and evict old blocks in certain StorageDir by LRU
 */
public class PartialLRUEvictor extends LRUEvictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE); 
  private BlockMetadataManager mMetaManager;

  public PartialLRUEvictor(BlockMetadataManager meta) {
    super(meta);
    mMetaManager = Preconditions.checkNotNull(meta);
  }

  @Override
  public EvictionPlan freeSpace(long availableBytes, BlockStoreLocation location)
      throws IOException {
    StorageDir selectedDir = getDirWithMaxFreeSpace(availableBytes, location);
    if (selectedDir == null) {
      LOG.error("Failed to freeSpace: No StorageDir has enough capacity of {} bytes",
          availableBytes);
      return null;
    }
    StorageTier parentTier = selectedDir.getParentTier();
    BlockStoreLocation destLocation = new BlockStoreLocation(parentTier.getTierAlias(),
        parentTier.getTierLevel(), selectedDir.getDirIndex());
    
    // Call freeSpace(long, BlockStoreLocation) in LRUEvictor
    return super.freeSpace(availableBytes, destLocation);
  }

  /**
   * Get StorageDir with max free space.
   * 
   * @param availableBytes space size to be requested
   * @param location location that the space will be allocated in
   * @return the StorageDir selected
   * @throws IOException
   */
  private StorageDir getDirWithMaxFreeSpace(long availableBytes, BlockStoreLocation location)
      throws IOException {
    long maxFreeSize = -1;
    StorageDir selectedDir = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : mMetaManager.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
              && dir.getAvailableBytes() > maxFreeSize) {
            selectedDir = dir;
            maxFreeSize = dir.getAvailableBytes();
          }
        }
      }
    } else {
      int tierAlias = location.tierAlias();
      StorageTier tier = mMetaManager.getTier(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        // Loop over all dirs in the given tier
        for (StorageDir dir : tier.getStorageDirs()) {
          if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
              && dir.getAvailableBytes() > maxFreeSize) {
            selectedDir = dir;
            maxFreeSize = dir.getAvailableBytes();
          }
        }
      } else {
        int dirIndex = location.dir();
        StorageDir dir = tier.getDir(dirIndex);
        if (dir.getCommittedBytes() + dir.getAvailableBytes() >= availableBytes
            && dir.getAvailableBytes() > maxFreeSize) {
          selectedDir = dir;
          maxFreeSize = dir.getAvailableBytes();
        }
      }
    }
    return selectedDir;
  }
}
