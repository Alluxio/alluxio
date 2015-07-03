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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * A simple evictor that evicts arbitrary blocks until the required size is met. This class serves
 * as an example to implement an Evictor.
 */
public class GreedyEvictor extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockMetadataManager mMetaManager;

  public GreedyEvictor(BlockMetadataManager metadata) {
    mMetaManager = Preconditions.checkNotNull(metadata);
  }

  @Override
  public EvictionPlan freeSpace(long availableBytes, BlockStoreLocation location)
      throws IOException {
    // 1. Select a StorageDir that has enough capacity for required bytes.
    StorageDir selectedDir = null;
    if (location.equals(BlockStoreLocation.anyTier())) {
      selectedDir = selectDirToEvictBlocksFromAnyTier(availableBytes);
    } else {
      int tierAlias = location.tierAlias();
      StorageTier tier = mMetaManager.getTier(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        selectedDir = selectDirToEvictBlocksFromTier(tier, availableBytes);
      } else {
        int dirIndex = location.dir();
        StorageDir dir = tier.getDir(dirIndex);
        if (canEvictBlocksFromDir(dir, availableBytes)) {
          selectedDir = dir;
        }
      }
    }
    if (selectedDir == null) {
      LOG.error("Failed to freeSpace: No StorageDir has enough capacity of {} bytes",
          availableBytes);
      return null;
    }

    // 2. Check if the selected StorageDir already has enough space.
    List<Pair<Long, BlockStoreLocation>> toTransfer =
        new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    long bytesAvailableInDir = selectedDir.getAvailableBytes();
    if (bytesAvailableInDir >= availableBytes) {
      // No need to evict anything, return an eviction plan with empty instructions.
      return new EvictionPlan(toTransfer, toEvict);
    }

    // 3. Collect victim blocks from the selected StorageDir. They could either be evicted or
    // moved.
    List<BlockMeta> victimBlocks = new ArrayList<BlockMeta>();
    for (BlockMeta block : selectedDir.getBlocks()) {
      victimBlocks.add(block);
      bytesAvailableInDir += block.getBlockSize();
      if (bytesAvailableInDir >= availableBytes) {
        break;
      }
    }

    // 4. Make best effort to transfer victim blocks to lower tiers rather than evict them.
    Map<StorageDir, Long> pendingBytesInDir = new HashMap<StorageDir, Long>();
    for (BlockMeta block : victimBlocks) {
      StorageTier fromTier = block.getParentDir().getParentTier();
      List<StorageTier> toTiers = mMetaManager.getTiersBelow(fromTier.getTierAlias());
      StorageDir toDir = selectDirToTransferBlock(block, toTiers, pendingBytesInDir);
      if (toDir == null) {
        // Not possible to transfer
        toEvict.add(block.getBlockId());
      } else {
        StorageTier toTier = toDir.getParentTier();
        toTransfer.add(new Pair<Long, BlockStoreLocation>(block.getBlockId(),
            new BlockStoreLocation(toTier.getTierAlias(), toTier.getTierLevel(), toDir
                .getDirIndex())));
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytesInDir.put(toDir, pendingBytesInDir.get(toDir) + block.getBlockSize());
        } else {
          pendingBytesInDir.put(toDir, block.getBlockSize());
        }
      }
    }
    return new EvictionPlan(toTransfer, toEvict);
  }

  // TODO: share this as a util function as it may be useful for other Evictors.
  private boolean canEvictBlocksFromDir(StorageDir dir, long availableBytes) {
    return dir.getAvailableBytes() + dir.getCommittedBytes() >= availableBytes;
  }

  private StorageDir selectDirToEvictBlocksFromAnyTier(long availableBytes) {
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (canEvictBlocksFromDir(dir, availableBytes)) {
          return dir;
        }
      }
    }
    return null;
  }

  private StorageDir selectDirToEvictBlocksFromTier(StorageTier tier, long availableBytes) {
    for (StorageDir dir : tier.getStorageDirs()) {
      if (canEvictBlocksFromDir(dir, availableBytes)) {
        return dir;
      }
    }
    return null;
  }

  private StorageDir selectDirToTransferBlock(BlockMeta block, List<StorageTier> toTiers,
      Map<StorageDir, Long> pendingBytesInDir) {
    for (StorageTier toTier : toTiers) {
      for (StorageDir toDir : toTier.getStorageDirs()) {
        long pendingBytes = 0;
        if (pendingBytesInDir.containsKey(toDir)) {
          pendingBytes = pendingBytesInDir.get(toDir);
        }
        if (toDir.getAvailableBytes() - pendingBytes >= block.getBlockSize()) {
          return toDir;
        }
      }
    }
    return null;
  }
}
