/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A simple evictor that evicts arbitrary blocks until the required size is met. This class serves
 * as an example to implement an Evictor. It does not use the block metadata information or the
 * allocation policy to determine which blocks should be evicted.
 */
@ThreadSafe
public final class GreedyEvictor implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Creates a new instance of {@link GreedyEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
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
      String tierAlias = location.tierAlias();
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
    List<BlockTransferInfo> toTransfer = new ArrayList<BlockTransferInfo>();
    List<Pair<Long, BlockStoreLocation>> toEvict = new ArrayList<Pair<Long, BlockStoreLocation>>();
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
      // TODO(qifan): Should avoid calling getParentDir.
      String fromTierAlias = block.getParentDir().getParentTier().getTierAlias();
      List<StorageTierView> candidateTiers = view.getTierViewsBelow(fromTierAlias);
      StorageDirView dstDir = selectAvailableDir(block, candidateTiers, pendingBytesInDir);
      if (dstDir == null) {
        // Not possible to transfer
        toEvict.add(new Pair<Long, BlockStoreLocation>(block.getBlockId(),
            block.getBlockLocation()));
      } else {
        StorageTierView dstTier = dstDir.getParentTierView();
        toTransfer.add(new BlockTransferInfo(block.getBlockId(), block.getBlockLocation(),
            new BlockStoreLocation(dstTier.getTierViewAlias(), dstDir.getDirViewIndex())));
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
