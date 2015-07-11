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
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTierView;

/**
 * A simple evictor that evicts arbitrary blocks until the required size is met. This class serves
 * as an example to implement an Evictor.
 */
public class GreedyEvictor extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMetadataManagerView mManagerView;

  public GreedyEvictor(BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view);
  }

  @Override
  public EvictionPlan freeSpaceWithView(long availableBytes, BlockStoreLocation location,
      BlockMetadataManagerView view) throws IOException {
    mManagerView = view;
    return freeSpace(availableBytes, location);
  }

  /**
   * This method should only be accessed by {@link freeSpaceWithView} in this class.
   * Frees space in the given block store location.
   * After eviction, at least one StorageDir in the location
   * has the specific amount of free space after eviction. The location can be a specific
   * StorageDir, or {@link BlockStoreLocation#anyTier} or {@link BlockStoreLocation#anyDirInTier}.
   * The view is generated and passed by the calling {@link BlockStore}.
   *
   * <P>
   * This method returns null if Evictor fails to propose a feasible plan to meet the requirement,
   * or an eviction plan with toMove and toEvict fields to indicate how to free space. If both
   * toMove and toEvict of the plan are empty, it indicates that Evictor has no actions to take and
   * the requirement is already met.
   *
   * @param availableBytes the amount of free space in bytes to be ensured after eviction
   * @param location the location in block store
   * @return an eviction plan (possibly with empty fields) to get the free space, or null if no plan
   *         is feasible
   * @throws IOException if given block location is invalid
   */
  private EvictionPlan freeSpace(long availableBytes, BlockStoreLocation location)
      throws IOException {
    // 1. Select a StorageDirView that has enough capacity for required bytes.
    StorageDirView selectedDirView = null;
    if (location.equals(BlockStoreLocation.anyTier())) {
      selectedDirView = selectDirToEvictBlocksFromAnyTier(availableBytes);
    } else {
      int tierAlias = location.tierAlias();
      StorageTierView tierView = mManagerView.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        selectedDirView = selectDirToEvictBlocksFromTier(tierView, availableBytes);
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
      List<StorageTierView> toTiers = mManagerView.getTierViewsBelow(fromTierAlias);
      StorageDirView toDir = selectDirToTransferBlock(block, toTiers, pendingBytesInDir);
      if (toDir == null) {
        // Not possible to transfer
        toEvict.add(block.getBlockId());
      } else {
        StorageTierView toTier = toDir.getParentTierView();
        toTransfer.add(new Pair<Long, BlockStoreLocation>(block.getBlockId(),
            new BlockStoreLocation(toTier.getTierViewAlias(), toTier.getTierViewLevel(), toDir
                .getDirViewIndex())));
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
  private boolean canEvictBlocksFromDir(StorageDirView dirView, long availableBytes) {
    return dirView.getAvailableBytes() + dirView.getEvitableBytes() >= availableBytes;
  }

  private StorageDirView selectDirToEvictBlocksFromAnyTier(long availableBytes) {
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (canEvictBlocksFromDir(dirView, availableBytes)) {
          return dirView;
        }
      }
    }
    return null;
  }

  private StorageDirView selectDirToEvictBlocksFromTier(StorageTierView tierView,
      long availableBytes) {
    for (StorageDirView dirView : tierView.getDirViews()) {
      if (canEvictBlocksFromDir(dirView, availableBytes)) {
        return dirView;
      }
    }
    return null;
  }

  private StorageDirView selectDirToTransferBlock(BlockMeta block, List<StorageTierView> toTiers,
      Map<StorageDirView, Long> pendingBytesInDir) {
    for (StorageTierView toTier : toTiers) {
      for (StorageDirView toDir : toTier.getDirViews()) {
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
