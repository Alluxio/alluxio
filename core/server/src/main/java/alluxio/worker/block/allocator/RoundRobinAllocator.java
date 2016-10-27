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

package alluxio.worker.block.allocator;

import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A round-robin allocator that allocates a block in the storage dir. It will allocate the block in
 * the highest tier possible: It always starts from the highest tier in a RR manner and goes to the
 * next tier when there is not enough space. The allocator only considers non-specific writes in its
 * RR policy (the location is either AnyTier or AnyDirInTier).
 */
@NotThreadSafe
public final class RoundRobinAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  // We need to remember the last dir index for every storage tier
  private Map<String, Integer> mTierAliasToLastDirMap = new HashMap<>();

  /**
   * Creates a new instance of {@link RoundRobinAllocator}.
   *
   * @param view {@link BlockMetadataManagerView} to pass to the allocator
   */
  public RoundRobinAllocator(BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view);
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      mTierAliasToLastDirMap.put(tierView.getTierViewAlias(), -1);
    }
  }

  @Override
  public StorageDirView allocateBlockWithView(long sessionId, long blockSize,
      BlockStoreLocation location, BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view);
    return allocateBlock(sessionId, blockSize, location);
  }

  /**
   * Allocates a block from the given block store location. The location can be a specific location,
   * or {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(String)}.
   *
   * @param sessionId the id of session to apply for the block allocation
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @return a {@link StorageDirView} in which to create the temp block meta if success, null
   *         otherwise
   * @throws IllegalArgumentException if block location is invalid
   */
  private StorageDirView allocateBlock(long sessionId, long blockSize,
      BlockStoreLocation location) {
    Preconditions.checkNotNull(location);
    if (location.equals(BlockStoreLocation.anyTier())) {
      int tierIndex = 0; // always starting from the first tier
      for (int i = 0; i < mManagerView.getTierViews().size(); i++) {
        StorageTierView tierView = mManagerView.getTierViews().get(tierIndex);
        int dirViewIndex = getNextAvailDirInTier(tierView, blockSize);
        if (dirViewIndex >= 0) {
          mTierAliasToLastDirMap.put(tierView.getTierViewAlias(), dirViewIndex);
          return tierView.getDirView(dirViewIndex);
        } else { // we didn't find one in this tier, go to next tier
          tierIndex++;
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      int dirViewIndex = getNextAvailDirInTier(tierView, blockSize);
      if (dirViewIndex >= 0) {
        mTierAliasToLastDirMap.put(tierView.getTierViewAlias(), dirViewIndex);
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
   * Finds an available dir in a given tier for a block with blockSize.
   *
   * @param tierView the tier to find a dir
   * @param blockSize the requested block size
   * @return the index of the dir if non-negative; -1 if fail to find a dir
   */
  private int getNextAvailDirInTier(StorageTierView tierView, long blockSize) {
    int dirViewIndex = mTierAliasToLastDirMap.get(tierView.getTierViewAlias());
    for (int i = 0; i < tierView.getDirViews().size(); i++) { // try this many times
      dirViewIndex = (dirViewIndex + 1) % tierView.getDirViews().size();
      if (tierView.getDirView(dirViewIndex).getAvailableBytes() >= blockSize) {
        return dirViewIndex;
      }
    }
    return -1;
  }
}
