/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.collections.Pair;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictableView;
import alluxio.worker.block.meta.StorageTierEvictableView;

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
  private static final Logger LOG = LoggerFactory.getLogger(GreedyEvictor.class);

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
    return freeSpaceWithView(availableBytes, location, view, Mode.GUARANTEED);
  }

  @Override
  public EvictionPlan freeSpaceWithView(long availableBytes, BlockStoreLocation location,
      BlockMetadataManagerView view, Mode mode) {
    Preconditions.checkNotNull(location, "location");
    Preconditions.checkNotNull(view, "view");

    // 1. Select a StorageDirEvictableView that has enough capacity for required bytes.
    StorageDirEvictableView selectedDirView = null;
    if (location.equals(BlockStoreLocation.anyTier())) {
      selectedDirView = selectEvictableDirFromAnyTier(view, availableBytes);
    } else {
      String tierAlias = location.tierAlias();
      StorageTierEvictableView tierView = view.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        selectedDirView = selectEvictableDirFromTier(tierView, availableBytes);
      } else {
        int dirIndex = location.dir();
        StorageDirEvictableView dir = tierView.getDirView(dirIndex);
        if (canEvictBlocksFromDir(dir, availableBytes)) {
          selectedDirView = dir;
        }
      }
    }
    if (selectedDirView == null) {
      LOG.warn("Failed to freeSpace: No StorageDirEvictableView has enough capacity of {} bytes",
          availableBytes);
      return null;
    }

    // 2. Check if the selected StorageDirEvictableView already has enough space.
    List<BlockTransferInfo> toTransfer = new ArrayList<>();
    List<Pair<Long, BlockStoreLocation>> toEvict = new ArrayList<>();
    long bytesAvailableInDir = selectedDirView.getAvailableBytes();
    if (bytesAvailableInDir >= availableBytes) {
      // No need to evict anything, return an eviction plan with empty instructions.
      return new EvictionPlan(toTransfer, toEvict);
    }

    // 3. Collect victim blocks from the selected StorageDirEvictableView.
    // They could either be evicted or moved.
    List<BlockMeta> victimBlocks = new ArrayList<>();
    for (BlockMeta block : selectedDirView.getEvictableBlocks()) {
      victimBlocks.add(block);
      bytesAvailableInDir += block.getBlockSize();
      if (bytesAvailableInDir >= availableBytes) {
        break;
      }
    }

    // 4. Make best effort to transfer victim blocks to lower tiers rather than evict them.
    Map<StorageDirEvictableView, Long> pendingBytesInDir = new HashMap<>();
    for (BlockMeta block : victimBlocks) {
      // TODO(qifan): Should avoid calling getParentDir.
      String fromTierAlias = block.getParentDir().getParentTier().getTierAlias();
      List<StorageTierEvictableView> candidateTiers = view.getTierViewsBelow(fromTierAlias);
      StorageDirEvictableView dstDir = selectAvailableDir(block, candidateTiers, pendingBytesInDir);
      if (dstDir == null) {
        // Not possible to transfer
        toEvict.add(new Pair<>(block.getBlockId(), block.getBlockLocation()));
      } else {
        StorageTierEvictableView dstTier = dstDir.getParentTierView();
        toTransfer.add(new BlockTransferInfo(block.getBlockId(), block.getBlockLocation(),
            new BlockStoreLocation(dstTier.getTierViewAlias(), dstDir.getDirViewIndex(),
                dstDir.getMediumType())));
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
  private boolean canEvictBlocksFromDir(StorageDirEvictableView dirView, long bytesToBeAvailable) {
    return dirView.getAvailableBytes() + dirView.getEvitableBytes() >= bytesToBeAvailable;
  }

  // Selects a dir with enough space (including space evictable) from all tiers
  private StorageDirEvictableView selectEvictableDirFromAnyTier(BlockMetadataManagerView view,
                                                                long availableBytes) {
    for (StorageTierEvictableView tierView : view.getTierViews()) {
      for (StorageDirEvictableView dirView : tierView.getDirViews()) {
        if (canEvictBlocksFromDir(dirView, availableBytes)) {
          return dirView;
        }
      }
    }
    return null;
  }

  // Selects a dir with enough space (including space evictable) from a given tier
  private StorageDirEvictableView selectEvictableDirFromTier(StorageTierEvictableView tierView,
      long availableBytes) {
    for (StorageDirEvictableView dirView : tierView.getDirViews()) {
      if (canEvictBlocksFromDir(dirView, availableBytes)) {
        return dirView;
      }
    }
    return null;
  }

  private StorageDirEvictableView selectAvailableDir(BlockMeta block,
      List<StorageTierEvictableView> candidateTiers,
      Map<StorageDirEvictableView, Long> pendingBytesInDir) {
    for (StorageTierEvictableView candidateTier : candidateTiers) {
      for (StorageDirEvictableView candidateDir : candidateTier.getDirViews()) {
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
