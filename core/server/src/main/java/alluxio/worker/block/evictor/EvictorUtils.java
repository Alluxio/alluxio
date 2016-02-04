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

package alluxio.worker.block.evictor;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

/**
 * Utility functions for the evictor package.
 */
@ThreadSafe
// TODO(calvin): This could be moved into EvictorBase.
public final class EvictorUtils {

  /**
   * Gets {@link StorageDirView} with max free space.
   *
   * @param bytesToBeAvailable space size to be requested
   * @param location location that the space will be allocated in
   * @param mManagerView a view of block metadata information
   * @return the {@link StorageDirView} selected
   */
  public static StorageDirView getDirWithMaxFreeSpace(long bytesToBeAvailable,
      BlockStoreLocation location, BlockMetadataManagerView mManagerView) {
    long maxFreeSize = -1;
    StorageDirView selectedDirView = null;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getCommittedBytes() + dirView.getAvailableBytes() >= bytesToBeAvailable
              && dirView.getAvailableBytes() > maxFreeSize) {
            selectedDirView = dirView;
            maxFreeSize = dirView.getAvailableBytes();
          }
        }
      }
    } else {
      String tierAlias = location.tierAlias();
      StorageTierView tierView = mManagerView.getTierView(tierAlias);
      if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getCommittedBytes() + dirView.getAvailableBytes() >= bytesToBeAvailable
              && dirView.getAvailableBytes() > maxFreeSize) {
            selectedDirView = dirView;
            maxFreeSize = dirView.getAvailableBytes();
          }
        }
      } else {
        int dirIndex = location.dir();
        StorageDirView dirView = tierView.getDirView(dirIndex);
        if (dirView.getCommittedBytes() + dirView.getAvailableBytes() >= bytesToBeAvailable
            && dirView.getAvailableBytes() > maxFreeSize) {
          selectedDirView = dirView;
          maxFreeSize = dirView.getAvailableBytes();
        }
      }
    }
    return selectedDirView;
  }

  /**
   * Finds a directory in the given location range with capacity upwards of the given bound.
   *
   * @param bytesToBeAvailable the capacity bound
   * @param location the location range
   * @param mManagerView the storage manager view
   * @return a {@link StorageDirView} in the range of location that already has availableBytes
   *         larger than bytesToBeAvailable, otherwise null
   */
  public static StorageDirView selectDirWithRequestedSpace(long bytesToBeAvailable,
      BlockStoreLocation location, BlockMetadataManagerView mManagerView) {
    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTierView tierView : mManagerView.getTierViews()) {
        for (StorageDirView dirView : tierView.getDirViews()) {
          if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
            return dirView;
          }
        }
      }
      return null;
    }

    String tierAlias = location.tierAlias();
    StorageTierView tierView = mManagerView.getTierView(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        if (dirView.getAvailableBytes() >= bytesToBeAvailable) {
          return dirView;
        }
      }
      return null;
    }

    StorageDirView dirView = tierView.getDirView(location.dir());
    return (dirView.getAvailableBytes() >= bytesToBeAvailable) ? dirView : null;
  }

  private EvictorUtils() {} // prevent instantiation
}
