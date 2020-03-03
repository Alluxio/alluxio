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

package alluxio.worker.block.meta;

import alluxio.worker.block.BlockMetadataEvictorView;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a wrapper of {@link StorageTier} to provide more limited access for evictors.
 */
@ThreadSafe
public class StorageTierEvictorView extends StorageTierView {

  /** The {@link BlockMetadataEvictorView} this {@link StorageTierEvictorView} is under. */
  private final BlockMetadataEvictorView mMetadataView;

  /**
   * Creates a {@link StorageTierEvictorView} using the actual {@link StorageTier} and the above
   * {@link BlockMetadataEvictorView}.
   *
   * @param tier which the tierView is constructed from
   * @param view the {@link BlockMetadataEvictorView} this tierView is associated with
   */
  public StorageTierEvictorView(StorageTier tier, BlockMetadataEvictorView view) {
    super(tier);
    mMetadataView = Preconditions.checkNotNull(view, "view");

    for (StorageDir dir : mTier.getStorageDirs()) {
      StorageDirEvictorView dirView = new StorageDirEvictorView(dir, this, view);
      mDirViews.put(dirView.getDirViewIndex(), dirView);
    }
  }

  /**
   * @return the block metadata evictor view for this storage tier view
   */
  public BlockMetadataEvictorView getBlockMetadataEvictorView() {
    return mMetadataView;
  }
}
