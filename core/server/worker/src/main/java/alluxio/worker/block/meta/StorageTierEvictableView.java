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

import alluxio.worker.block.BlockMetadataManagerView;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a wrapper of {@link StorageTier} to provide more limited access.
 */
@ThreadSafe
public final class StorageTierEvictableView {

  /** The {@link StorageTier} this view is derived from. */
  private final StorageTier mTier;
  /** A list of {@link StorageDirEvictableView} under this StorageTierEvictableView. */
  private final List<StorageDirEvictableView> mDirViews = new ArrayList<>();
  /** The {@link BlockMetadataManagerView} this {@link StorageTierEvictableView} is under. */
  private final BlockMetadataManagerView mManagerView;

  /**
   * Creates a {@link StorageTierEvictableView} using the actual {@link StorageTier} and the above
   * {@link BlockMetadataManagerView}.
   *
   * @param tier which the tierView is constructed from
   * @param view the {@link BlockMetadataManagerView} this tierView is associated with
   */
  public StorageTierEvictableView(StorageTier tier, BlockMetadataManagerView view) {
    mTier = Preconditions.checkNotNull(tier, "tier");
    mManagerView = Preconditions.checkNotNull(view, "view");

    for (StorageDir dir : mTier.getStorageDirs()) {
      StorageDirEvictableView dirView = new StorageDirEvictableView(dir, this, view);
      mDirViews.add(dirView);
    }
  }

  /**
   * @return a list of directory views in this storage tier view
   */
  public List<StorageDirEvictableView> getDirViews() {
    return Collections.unmodifiableList(mDirViews);
  }

  /**
   * Returns a directory view for the given index.
   *
   * @param dirIndex the directory view index
   * @return a directory view
   */
  public StorageDirEvictableView getDirView(int dirIndex) {
    return mDirViews.get(dirIndex);
  }

  /**
   * @return the storage tier view alias
   */
  public String getTierViewAlias() {
    return mTier.getTierAlias();
  }

  /**
   * @return the ordinal value of the storage tier view
   */
  public int getTierViewOrdinal() {
    return mTier.getTierOrdinal();
  }

  /**
   * @return the block metadata manager view for this storage tier view
   */
  public BlockMetadataManagerView getBlockMetadataManagerView() {
    return mManagerView;
  }
}
