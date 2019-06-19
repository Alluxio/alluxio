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
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An allocator that allocates a block in the storage dir with most free space. It always allocates
 * to the highest tier if the requested block store location is any tier.
 */
@NotThreadSafe
public final class MaxFreeAllocator implements Allocator {
  private BlockMetadataManagerView mManagerView;

  /**
   * Creates a new instance of {@link MaxFreeAllocator}.
   *
   * @param view {@link BlockMetadataManagerView} to pass to the allocator
   */
  public MaxFreeAllocator(BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view, "view");
  }

  @Override
  public StorageDir allocateBlockWithTierInfo(long sessionId, long blockSize,
      BlockStoreLocation location, List<StorageTier> tierList,
      Map<String, StorageTier> aliasToTiersMap) {
    return allocateBlock(sessionId, blockSize, location, tierList, aliasToTiersMap);
  }

  @Override
  public StorageDirView allocateBlockWithView(long sessionId, long blockSize,
      BlockStoreLocation location, BlockMetadataManagerView view) {
    mManagerView = Preconditions.checkNotNull(view, "view");
    return allocateBlock(sessionId, blockSize, location);
  }

  /**
   * Allocates a block from the given block store location. The location can be a specific location,
   * or {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(String)}.
   *
   * @param sessionId the id of session to apply for the block allocation
   * @param blockSize the size of block in bytes
   * @param location the location in block store
   * @param tierList a list of {@link StorageTier}
   * @param aliasToTiersMap a map from tier alias to {@link StorageTier}
   * @return a {@link StorageDir} in which to create the temp block meta if success, null
   *         otherwise
   * @throws IllegalArgumentException if block location is invalid
   */
  private StorageDir allocateBlock(long sessionId, long blockSize,
      BlockStoreLocation location, List<StorageTier> tierList,
      Map<String, StorageTier> aliasToTiersMap) {
    Preconditions.checkNotNull(location, "location");
    StorageDir candidateDir = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : tierList) {
        candidateDir = getCandidateDirInTier(tier, blockSize,
            BlockStoreLocation.ANY_MEDIUM);
        if (candidateDir != null) {
          break;
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTier tier = aliasToTiersMap.get(location.tierAlias());
      candidateDir = getCandidateDirInTier(tier, blockSize, BlockStoreLocation.ANY_MEDIUM);
    } else if (location.equals(BlockStoreLocation.anyDirInTierWithMedium(location.mediumType()))) {
      for (StorageTier tier : tierList) {
        candidateDir = getCandidateDirInTier(tier, blockSize, location.mediumType());
        if (candidateDir != null) {
          break;
        }
      }
    } else {
      StorageTier tier = aliasToTiersMap.get(location.tierAlias());
      StorageDir dir = tier.getDir(location.dir());
      if (dir.getAvailableBytes() >= blockSize) {
        candidateDir = dir;
      }
    }

    return candidateDir;
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
    Preconditions.checkNotNull(location, "location");
    StorageDirView candidateDirView = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        candidateDirView = getCandidateDirInTier(tierView, blockSize,
            BlockStoreLocation.ANY_MEDIUM);
        if (candidateDirView != null) {
          break;
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      candidateDirView = getCandidateDirInTier(tierView, blockSize, BlockStoreLocation.ANY_MEDIUM);
    } else if (location.equals(BlockStoreLocation.anyDirInTierWithMedium(location.mediumType()))) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        candidateDirView = getCandidateDirInTier(tierView, blockSize, location.mediumType());
        if (candidateDirView != null) {
          break;
        }
      }
    } else {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      StorageDirView dirView = tierView.getDirView(location.dir());
      if (dirView.getAvailableBytes() >= blockSize) {
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
   * @param mediumType the medium type that must match
   * @return the storage directory view if found, null otherwise
   */
  private StorageDirView getCandidateDirInTier(StorageTierView tierView, long blockSize,
      String mediumType) {
    StorageDirView candidateDirView = null;
    long maxFreeBytes = blockSize - 1;
    for (StorageDirView dirView : tierView.getDirViews()) {
      if ((mediumType.equals(BlockStoreLocation.ANY_MEDIUM)
          || dirView.getMediumType().equals(mediumType))
          && dirView.getAvailableBytes() > maxFreeBytes) {
        maxFreeBytes = dirView.getAvailableBytes();
        candidateDirView = dirView;
      }
    }
    return candidateDirView;
  }

  /**
   * Finds a directory in a tier that has max free space and is able to store the block.
   *
   * @param tier the storage tier
   * @param blockSize the size of block in bytes
   * @param mediumType the medium type that must match
   * @return the storage directory if found, null otherwise
   */
  private StorageDir getCandidateDirInTier(StorageTier tier, long blockSize,
      String mediumType) {
    StorageDir candidateDir = null;
    long maxFreeBytes = blockSize - 1;
    for (StorageDir dir : tier.getStorageDirs()) {
      if ((mediumType.equals(BlockStoreLocation.ANY_MEDIUM)
          || dir.getDirMedium().equals(mediumType))
          && dir.getAvailableBytes() > maxFreeBytes) {
        maxFreeBytes = dir.getAvailableBytes();
        candidateDir = dir;
      }
    }
    return candidateDir;
  }
}
