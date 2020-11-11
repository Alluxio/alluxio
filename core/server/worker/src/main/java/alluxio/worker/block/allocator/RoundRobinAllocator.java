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
import java.util.List;
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
  private static final Logger LOG = LoggerFactory.getLogger(GreedyAllocator.class);

  private BlockMetadataView mMetadataView;
  private Reviewer mReviewer;

  // We need to remember the last dir index for every storage tier
  private Map<String, Integer> mTierAliasToLastDirMap = new HashMap<>();

  /**
   * Creates a new instance of {@link RoundRobinAllocator}.
   *
   * @param view {@link BlockMetadataView} to pass to the allocator
   */
  public RoundRobinAllocator(BlockMetadataView view) {
    mMetadataView = Preconditions.checkNotNull(view, "view");
    for (StorageTierView tierView : mMetadataView.getTierViews()) {
      mTierAliasToLastDirMap.put(tierView.getTierViewAlias(), -1);
    }
    mReviewer = Reviewer.Factory.create();
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
   * @throws IllegalArgumentException if block location is invalid
   */
  @Nullable
  private StorageDirView allocateBlock(long sessionId, long blockSize,
      BlockStoreLocation location) {
    Preconditions.checkNotNull(location, "location");
    if (location.equals(BlockStoreLocation.anyTier())) {
      int tierIndex = 0; // always starting from the first tier
      for (int i = 0; i < mMetadataView.getTierViews().size(); i++) {
        StorageTierView tierView = mMetadataView.getTierViews().get(tierIndex);
        int dirViewIndex = getNextAvailDirInTier(tierView, blockSize,
            BlockStoreLocation.ANY_MEDIUM);
        if (dirViewIndex >= 0) {
          StorageDirView candidateDir = tierView.getDirView(dirViewIndex);
          if (mReviewer.reviewAllocation(candidateDir)) {
            mTierAliasToLastDirMap.put(tierView.getTierViewAlias(), dirViewIndex);
            return candidateDir;
          } else {
            // Try the next dir
            LOG.debug("Allocation rejected for anyTier: {}",
                    candidateDir.toBlockStoreLocation());
          }
        } else { // we didn't find one in this tier, go to next tier
          tierIndex++;
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tierView = mMetadataView.getTierView(location.tierAlias());
      int offset = 0;
      while (offset < tierView.getDirViews().size() && offset > -1) {
        int dirViewIndex = getNextAvailDirInTier(tierView, blockSize, BlockStoreLocation.ANY_MEDIUM);
        if (dirViewIndex >= 0) {
          StorageDirView candidateDir = tierView.getDirView(dirViewIndex);
          if (mReviewer.reviewAllocation(candidateDir)) {
            mTierAliasToLastDirMap.put(tierView.getTierViewAlias(), dirViewIndex);
            return tierView.getDirView(dirViewIndex);
          } else {
            // Reject the allocation
            LOG.debug("Allocation to dirIndex {} rejected for anyDirInTier: {}", dirViewIndex,
                    candidateDir.toBlockStoreLocation());
          }
        }
        // Either the dirViewIndex is -1, or we rejected the allocation to a dirViewIndex > 0
        offset = dirViewIndex;
      }
    } else if (location.equals(BlockStoreLocation.anyDirInAnyTierWithMedium(
                location.mediumType()))) {
      StorageTierView tierView = mMetadataView.getTierView(location.tierAlias());
      int offset = 0;

      String medium = location.mediumType();
      int tierIndex = 0; // always starting from the first tier
      for (int i = 0; i < mMetadataView.getTierViews().size(); i++) {
      }

      while (offset < tierView.getDirViews().size() && offset > -1) {
        int dirViewIndex = getNextAvailDirInTier(tierView, blockSize, location.mediumType());
        if (dirViewIndex >= 0) {
          StorageDirView candidateDir = tierView.getDirView(dirViewIndex);
          if (mReviewer.reviewAllocation(candidateDir)) {
            mTierAliasToLastDirMap.put(tierView.getTierViewAlias(), dirViewIndex);
            return tierView.getDirView(dirViewIndex);
          } else {
            // Reject the allocation
            LOG.debug("Allocation to dirIndex {} rejected for anyDirInTier: {}", dirViewIndex,
                    candidateDir.toBlockStoreLocation());
          }
        }
        // Either the dirViewIndex is -1, or we rejected the allocation to a dirViewIndex > 0
        offset = dirViewIndex;
      }
    } else {
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
   * @param mediumType the medium type to find a dir
   * @return the index of the dir if non-negative; -1 if fail to find a dir
   */
  private int getNextAvailDirInTier(StorageTierView tierView, long blockSize, String mediumType) {
    return getNextAvailDirInTier(tierView, blockSize, mediumType, 0);
  }

  /**
   * Finds an available dir in a given tier for a block with blockSize.
   *
   * @param tierView the tier to find a dir
   * @param blockSize the requested block size
   * @param mediumType the medium type to find a dir
   * @param offset skip this many directories
   * @return the index of the dir if non-negative; -1 if fail to find a dir
   */
  private int getNextAvailDirInTier(StorageTierView tierView, long blockSize, String mediumType, int offset) {
    // None of the dirs in the tier can satisfy
    if (offset >= tierView.getDirViews().size()) {
      return -1;
    }

    int dirIndex = mTierAliasToLastDirMap.get(tierView.getTierViewAlias()) + offset;
    List<StorageDirView> dirs = tierView.getDirViews();
    for (int i = offset; i < dirs.size(); i++) { // try this many times
      dirIndex = (dirIndex + 1) % dirs.size();
      StorageDirView dir = dirs.get(dirIndex);
      if ((mediumType.equals(BlockStoreLocation.ANY_MEDIUM)
              || dir.getMediumType().equals(mediumType))
              && dir.getAvailableBytes() >= blockSize) {
        return dir.getDirViewIndex();
      }
    }
    return -1;
  }
}
