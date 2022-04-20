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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An allocator that allocates a block in the storage dir with most free space. It always allocates
 * to the highest tier if the requested block store location is any tier.
 */
@NotThreadSafe
public final class MaxFreeAllocator implements Allocator {
  private static final Logger LOG = LoggerFactory.getLogger(MaxFreeAllocator.class);

  private BlockMetadataView mMetadataView;
  private Reviewer mReviewer;

  /**
   * Creates a new instance of {@link MaxFreeAllocator}.
   *
   * @param view {@link BlockMetadataView} to pass to the allocator
   */
  public MaxFreeAllocator(BlockMetadataView view) {
    mMetadataView = Preconditions.checkNotNull(view, "view");
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
   * @param skipReview whether the review should be skipped
   * @return a {@link StorageDirView} in which to create the temp block meta if success,
   *         null otherwise
   */
  private StorageDirView allocateBlock(long blockSize,
      BlockStoreLocation location, boolean skipReview) {
    Preconditions.checkNotNull(location, "location");
    StorageDirView candidateDirView = null;

    if (location.isAnyTier() && location.isAnyDir()) {
      for (StorageTierView tierView : mMetadataView.getTierViews()) {
        candidateDirView = getCandidateDirInTier(tierView, blockSize, location);
        if (candidateDirView != null) {
          if (skipReview || mReviewer.acceptAllocation(candidateDirView)) {
            break;
          }
          // We tried the dir on this tier with max free bytes but that is not good enough.
          // So we move on to the lower tier.
          LOG.debug("Allocation rejected for anyTier: {}",
                  candidateDirView.toBlockStoreLocation());
        }
      }
    } else if (location.isAnyDirWithTier()) {
      StorageTierView tierView = mMetadataView.getTierView(location.tierAlias());
      candidateDirView = getCandidateDirInTier(tierView, blockSize, location);
      if (candidateDirView != null) {
        // The allocation is not good enough. Revert it.
        if (!skipReview && !mReviewer.acceptAllocation(candidateDirView)) {
          LOG.debug("Allocation rejected for anyDirInTier: {}",
                  candidateDirView.toBlockStoreLocation());
          candidateDirView = null;
        }
      }
    } else {
      // For allocation in a specific directory, we are not checking the reviewer,
      // because we do not want the reviewer to reject it.
      StorageTierView tierView = mMetadataView.getTierView(location.tierAlias());
      StorageDirView dirView = tierView.getDirView(location.dir());
      if (dirView != null && dirView.getAvailableBytes() >= blockSize) {
        candidateDirView = dirView;
      }
    }

    return candidateDirView;
  }

  /**
   * Finds a directory view in a tier view that has max free space and is able to store the block.
   *
   * @param tierView the storage tier view
   * @param blockSize the size of block in bytes
   * @param location the block store location
   * @return the storage directory view if found, null otherwise
   */
  private StorageDirView getCandidateDirInTier(StorageTierView tierView,
      long blockSize, BlockStoreLocation location) {
    StorageDirView candidateDirView = null;
    long maxFreeBytes = blockSize - 1;
    for (StorageDirView dirView : tierView.getDirViews()) {
      if ((location.isAnyMedium()
          || dirView.getMediumType().equals(location.mediumType()))
          && dirView.getAvailableBytes() > maxFreeBytes) {
        maxFreeBytes = dirView.getAvailableBytes();
        candidateDirView = dirView;
      }
    }
    return candidateDirView;
  }
}
