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

import alluxio.worker.block.BlockMetadataEvictableView;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a wrapper of {@link StorageTier} to provide more limited access.
 */
@ThreadSafe
public final class StorageTierEvictableView extends StorageTierView {

  /** The {@link BlockMetadataEvictableView} this {@link StorageTierEvictableView} is under. */
  private final BlockMetadataEvictableView mManagerView;

  /**
   * Creates a {@link StorageTierEvictableView} using the actual {@link StorageTier} and the above
   * {@link BlockMetadataEvictableView}.
   *
   * @param tier which the tierView is constructed from
   * @param view the {@link BlockMetadataEvictableView} this tierView is associated with
   */
  public StorageTierEvictableView(StorageTier tier, BlockMetadataEvictableView view) {
    super(tier, true);
    mManagerView = Preconditions.checkNotNull(view, "view");

    for (StorageDir dir : mTier.getStorageDirs()) {
      StorageDirEvictableView dirView = new StorageDirEvictableView(dir, this, view);
      mDirViews.add(dirView);
    }
  }

  /**
   * @return the block metadata manager view for this storage tier view
   */
  public BlockMetadataEvictableView getBlockMetadataEvictableView() {
    return mManagerView;
  }
}
