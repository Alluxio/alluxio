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

package alluxio.worker.block.allocator;

import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Preconditions;

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
    mManagerView = Preconditions.checkNotNull(view);
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
    StorageDirView candidateDirView = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        candidateDirView = getCandidateDirInTier(tierView, blockSize);
        if (candidateDirView != null) {
          break;
        }
      }
    } else if (location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      StorageTierView tierView = mManagerView.getTierView(location.tierAlias());
      candidateDirView = getCandidateDirInTier(tierView, blockSize);
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
   * @return the storage directory view if found, null otherwise
   */
  private StorageDirView getCandidateDirInTier(StorageTierView tierView, long blockSize) {
    StorageDirView candidateDirView = null;
    long maxFreeBytes = blockSize - 1;
    for (StorageDirView dirView : tierView.getDirViews()) {
      if (dirView.getAvailableBytes() > maxFreeBytes) {
        maxFreeBytes = dirView.getAvailableBytes();
        candidateDirView = dirView;
      }
    }
    return candidateDirView;
  }
}
