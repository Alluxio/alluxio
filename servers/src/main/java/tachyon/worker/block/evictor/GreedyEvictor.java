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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;

/**
 * A simple evictor that evicts arbitrary blocks until the required size is met. This class serves
 * as an example to implement an Evictor.
 */
public final class GreedyEvictor implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * GreedyEvictor does not need BlockMetadataManagerView
   */
  public GreedyEvictor(BlockMetadataManagerView view, Allocator allocator) {}

  @Override
  public EvictionPlan freeSpaceWithView(long availableBytes, BlockStoreLocation location,
      BlockMetadataManagerView view) {
    Preconditions.checkNotNull(location);
    Preconditions.checkNotNull(view);

    // 1. Select a StorageDirView that has enough capacity for required bytes.
    StorageDirView selectedDirView = null;
    if (location.equals(BlockStoreLocation.anyTier())) {
      selectedDirView = selectEvictableDirFromAnyTier(view, availableBytes);
    } else {
      int tierAlias = location.tierAlias();
      StorageTierView tierView = view.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        selectedDirView = selectEvictableDirFromTier(tierView, availableBytes);
      } else {
        int dirIndex = location.dir();
        StorageDirView dir = tierView.getDirView(dirIndex);
        if (canEvictBlocksFromDir(dir, availableBytes)) {
          selectedDirView = dir;
        }
      }
    }
    if (selectedDirView == null) {
      LOG.error("Failed to freeSpace: No StorageDirView has enough capacity of {} bytes",
          availableBytes);
      return null;
    }

    // 2. Check if the selected StorageDirView already has enough space.
    List<Pair<Long, BlockStoreLocation>> toTransfer =
        new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    long bytesAvailableInDir = selectedDirView.getAvailableBytes();
    if (bytesAvailableInDir >= availableBytes) {
      // No need to evict anything, return an eviction plan with empty instructions.
      return new EvictionPlan(toTransfer, toEvict);
    }

    // 3. Collect victim blocks from the selected StorageDirView. They could either be evicted or
    // moved.
    List<BlockMeta> victimBlocks = new ArrayList<BlockMeta>();
    for (BlockMeta block : selectedDirView.getEvictableBlocks()) {
      victimBlocks.add(block);
      bytesAvailableInDir += block.getBlockSize();
      if (bytesAvailableInDir >= availableBytes) {
        break;
      }
    }

    // 4. Make best effort to transfer victim blocks to lower tiers rather than evict them.
    Map<StorageDirView, Long> pendingBytesInDir = new HashMap<StorageDirView, Long>();
    for (BlockMeta block : victimBlocks) {
      // TODO: should avoid calling getParentDir
      int fromTierAlias = block.getParentDir().getParentTier().getTierAlias();
      List<StorageTierView> candidateTiers = view.getTierViewsBelow(fromTierAlias);
      StorageDirView dstDir = selectAvailableDir(block, candidateTiers, pendingBytesInDir);
      if (dstDir == null) {
        // Not possible to transfer
        toEvict.add(block.getBlockId());
      } else {
        StorageTierView dstTier = dstDir.getParentTierView();
        toTransfer
            .add(new Pair<Long, BlockStoreLocation>(block.getBlockId(), new BlockStoreLocation(
                dstTier.getTierViewAlias(), dstTier.getTierViewLevel(), dstDir.getDirViewIndex())));
        if (pendingBytesInDir.containsKey(dstDir)) {
          pendingBytesInDir.put(dstDir, pendingBytesInDir.get(dstDir) + block.getBlockSize());
        } else {
          pendingBytesInDir.put(dstDir, block.getBlockSize());
        }
      }
    }
    return new EvictionPlan(toTransfer, toEvict);
  }

  // Checks if a dir has enough space---including space already available and space might be
  // available after eviction.
  private boolean canEvictBlocksFromDir(StorageDirView dirView, long bytesToBeAvailable) {
    return dirView.getAvailableBytes() + dirView.getEvitableBytes() >= bytesToBeAvailable;
  }

  // Selects a dir with enough space (including space evictable) from all tiers
  private StorageDirView selectEvictableDirFromAnyTier(BlockMetadataManagerView view,
      long availableBytes) {
    for (StorageTierView tierView : view.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (canEvictBlocksFromDir(dirView, availableBytes)) {
          return dirView;
        }
      }
    }
    return null;
  }

  // Selects a dir with enough space (including space evictable) from a given tier
  private StorageDirView selectEvictableDirFromTier(StorageTierView tierView, long availableBytes) {
    for (StorageDirView dirView : tierView.getDirViews()) {
      if (canEvictBlocksFromDir(dirView, availableBytes)) {
        return dirView;
      }
    }
    return null;
  }

  private StorageDirView selectAvailableDir(BlockMeta block, List<StorageTierView> candidateTiers,
      Map<StorageDirView, Long> pendingBytesInDir) {
    for (StorageTierView candidateTier : candidateTiers) {
      for (StorageDirView candidateDir : candidateTier.getDirViews()) {
        long pendingBytes = 0;
        if (pendingBytesInDir.containsKey(candidateDir)) {
          pendingBytes = pendingBytesInDir.get(candidateDir);
        }
        if (candidateDir.getAvailableBytes() - pendingBytes >= block.getBlockSize()) {
          return candidateDir;
        }
      }
    }
    return null;
  }
}
