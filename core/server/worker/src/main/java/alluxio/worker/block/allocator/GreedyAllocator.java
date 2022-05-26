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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A greedy allocator that returns the first Storage dir fitting the size of block to allocate. This
 * class serves as an example how to implement an allocator.
 */
@NotThreadSafe
public final class GreedyAllocator implements Allocator {
  private static final Logger LOG = LoggerFactory.getLogger(GreedyAllocator.class);

  private BlockMetadataView mMetadataView;
  private Reviewer mReviewer;

  /**
   * Creates a new instance of {@link GreedyAllocator}.
   *
   * @param view {@link BlockMetadataView} to pass to the allocator
   */
  public GreedyAllocator(BlockMetadataView view) {
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
   * @return a {@link StorageDirView} in which to create the temp block meta if success,
   *         null otherwise
   */
  @Nullable
  private StorageDirView allocateBlock(long blockSize,
      BlockStoreLocation location, boolean skipReview) {
    Preconditions.checkNotNull(location, "location");
    if (location.isAnyTier() && location.isAnyDir()) {
      // When any tier is ok, loop over all tier views and dir views,
      // and return a temp block meta from the first available dirview.
      for (StorageTierView tierView : mMetadataView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if ((location.isAnyMedium()
              || dirView.getMediumType().equals(location.mediumType()))
              && dirView.getAvailableBytes() >= blockSize) {
            if (skipReview || mReviewer.acceptAllocation(dirView)) {
              return dirView;
            } else {
              // The allocation is rejected. Try the next dir.
              LOG.debug("Allocation rejected for anyTier: {}", dirView.toBlockStoreLocation());
            }
          }
        }
      }
      return null;
    }

    String tierAlias = location.tierAlias();
    StorageTierView tierView = mMetadataView.getTierView(tierAlias);
    if (location.isAnyDirWithTier()) {
      // Loop over all dir views in the given tier
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= blockSize) {
          if (skipReview || mReviewer.acceptAllocation(dirView)) {
            return dirView;
          } else {
            // Try the next dir
            LOG.debug("Allocation rejected for anyDirInTier: {}",
                    dirView.toBlockStoreLocation());
          }
        }
      }
      return null;
    }

    // For allocation in a specific directory, we are not checking the reviewer,
    // because we do not want the reviewer to reject it.
    int dirIndex = location.dir();
    StorageDirView dirView = tierView.getDirView(dirIndex);
    if (dirView != null && dirView.getAvailableBytes() >= blockSize) {
      return dirView;
    }
    return null;
  }
}
