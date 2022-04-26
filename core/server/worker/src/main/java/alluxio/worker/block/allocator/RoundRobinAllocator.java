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

import alluxio.worker.block.BlockMetadataView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;
import alluxio.worker.block.reviewer.Reviewer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A round-robin allocator that allocates a block in the storage dir. It will allocate the block in
 * the highest tier possible: It always starts from the highest tier in a RR manner and goes to the
 * next tier when there is not enough space. The allocator only considers non-specific writes in its
 * RR policy (the location is either AnyTier or AnyDirInTier).
 */
@NotThreadSafe
public final class RoundRobinAllocator implements Allocator {
  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinAllocator.class);

  private BlockMetadataView mMetadataView;
  private Reviewer mReviewer;

  // We need to remember the last dir index for every storage tier
  private Map<String, Iterator<StorageDirView>> mTierAliasToDirIteratorMap = new HashMap<>();

  /**
   * Creates a new instance of {@link RoundRobinAllocator}.
   *
   * @param view {@link BlockMetadataView} to pass to the allocator
   */
  public RoundRobinAllocator(BlockMetadataView view) {
    mMetadataView = Preconditions.checkNotNull(view, "view");
    for (StorageTierView tierView : mMetadataView.getTierViews()) {
      mTierAliasToDirIteratorMap.put(
          tierView.getTierViewAlias(), tierView.getDirViews().iterator());
    }
    mReviewer = Reviewer.Factory.create();
  }

  @Override
  public StorageDirView allocateBlockWithView(long blockSize,
      BlockStoreLocation location, BlockMetadataView metadataView, boolean skipReview) {
    mMetadataView = Preconditions.checkNotNull(metadataView, "view");
    return allocateBlock(blockSize, location, skipReview);
  }

  /**
   * Allocates a block from the given block store location. The location can be a specific location,
   * or {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(String)}.
   *
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @return a {@link StorageDirView} in which to create the temp block meta if success,
   *         null otherwise
   * @throws IllegalArgumentException if block location is invalid
   */
  @Nullable
  private StorageDirView allocateBlock(long blockSize,
      BlockStoreLocation location, boolean skipReview) {
    Preconditions.checkNotNull(location, "location");
    if (location.isAnyDirWithTier()) {
      StorageTierView tierView = mMetadataView.getTierView(location.tierAlias());
      // The review logic is handled in getNextAvailDirInTier
      return getNextAvailDirInTier(tierView, blockSize, location, skipReview);
    } else if (location.isAnyTier() && location.isAnyDir()) {
      for (StorageTierView tierView : mMetadataView.getTierViews()) {
        // The review logic is handled in getNextAvailDirInTier
        StorageDirView dir = getNextAvailDirInTier(tierView, blockSize, location, skipReview);
        if (dir != null) {
          return dir;
        }
      }
    } else {
      // For allocation in a specific directory, we are not checking the reviewer,
      // because we do not want the reviewer to reject it.
      StorageTierView tierView = mMetadataView.getTierView(location.tierAlias());
      StorageDirView dirView = tierView.getDirView(location.dir());
      if (dirView != null && dirView.getAvailableBytes() >= blockSize) {
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
   * @param location the block store location
   * @return the index of the dir if non-negative; -1 if fail to find a dir
   */
  private StorageDirView getNextAvailDirInTier(StorageTierView tierView, long blockSize,
      BlockStoreLocation location, boolean skipReview) {
    Iterator<StorageDirView> iterator = mTierAliasToDirIteratorMap.get(tierView.getTierViewAlias());
    int processed = 0;
    while (processed < tierView.getDirViews().size()) {
      if (!iterator.hasNext()) {
        iterator = tierView.getDirViews().iterator();
        mTierAliasToDirIteratorMap.put(tierView.getTierViewAlias(), iterator);
      }
      StorageDirView dir = iterator.next();
      if ((location.isAnyMedium()
          || dir.getMediumType().equals(location.mediumType()))
          && dir.getAvailableBytes() >= blockSize) {
        if (skipReview || mReviewer.acceptAllocation(dir)) {
          return dir;
        }
        // The allocation is rejected. Try the next dir.
        LOG.debug("Allocation to dir {} rejected: {}", dir,
            dir.toBlockStoreLocation());
      }
      processed++;
    }
    return null;
  }
}
