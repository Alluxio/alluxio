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

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.Nullable;

/**
 * A greedy allocator that returns the first Storage dir fitting the size of block to allocate. This
 * class serves as an example how to implement an allocator.
 */
@NotThreadSafe
public final class GreedyAllocator implements Allocator {
  private BlockMetadataView mMetadataView;

  /**
   * Creates a new instance of {@link GreedyAllocator}.
   *
   * @param view {@link BlockMetadataView} to pass to the allocator
   */
  public GreedyAllocator(BlockMetadataView view) {
    mMetadataView = Preconditions.checkNotNull(view, "view");
  }

  @Override
  public StorageDirView allocateBlockWithView(long sessionId, long blockSize,
      BlockStoreLocation location, BlockMetadataView metadataView) {
    mMetadataView = Preconditions.checkNotNull(metadataView, "view");
    return allocateBlock(sessionId, blockSize, location);
  }

  /**
   * Allocates a block from the given block store location. The location can be a specific location,
   * or {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(String)}.
   *
   * @param sessionId the id of session to apply for the block allocation
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @return a {@link StorageDirView} in which to create the temp block meta if success,
   *         null otherwise
   */
  @Nullable
  private StorageDirView allocateBlock(long sessionId, long blockSize,
      BlockStoreLocation location) {
    Preconditions.checkNotNull(location, "location");
    if (location.equals(BlockStoreLocation.anyTier())) {
      // When any tier is ok, loop over all tier views and dir views,
      // and return a temp block meta from the first available dirview.
      for (StorageTierView tierView : mMetadataView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= blockSize) {
            return dirView;
          }
        }
      }
      return null;
    }

    String mediumType = location.mediumType();
    if (!mediumType.equals(BlockStoreLocation.ANY_MEDIUM)
        && location.equals(BlockStoreLocation.anyDirInTierWithMedium(mediumType))) {
      for (StorageTierView tierView : mMetadataView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getMediumType().equals(mediumType)
              && dirView.getAvailableBytes() >= blockSize) {
            return dirView;
          }
        }
      }
      return null;
    }

    String tierAlias = location.tierAlias();
    StorageTierView tierView = mMetadataView.getTierView(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      // Loop over all dir views in the given tier
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= blockSize) {
          return dirView;
        }
      }
      return null;
    }

    int dirIndex = location.dir();
    StorageDirView dirView = tierView.getDirView(dirIndex);
    if (dirView.getAvailableBytes() >= blockSize) {
      return dirView;
    }
    return null;
  }
}
