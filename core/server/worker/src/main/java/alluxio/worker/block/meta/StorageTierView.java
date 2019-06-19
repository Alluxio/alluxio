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

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a wrapper of {@link StorageTier} to provide more limited access.
 */
@ThreadSafe
public final class StorageTierView {

  /** The {@link StorageTier} this view is derived from. */
  private final StorageTier mTier;
  /** A list of {@link StorageDirView} under this StorageTierView. */
  private final List<StorageDirView> mDirViews = new ArrayList<>();

  /**
   * Creates a {@link StorageTierView} using the actual {@link StorageTier}.
   *
   * @param tier which the tierView is constructed from
   */
  public StorageTierView(StorageTier tier) {
    mTier = Preconditions.checkNotNull(tier, "tier");

    for (StorageDir dir : mTier.getStorageDirs()) {
      StorageDirView dirView = new StorageDirView(dir, this);
      mDirViews.add(dirView);
    }
  }

  /**
   * @return a list of directory views in this storage tier view
   */
  public List<StorageDirView> getDirViews() {
    return Collections.unmodifiableList(mDirViews);
  }

  /**
   * Returns a directory view for the given index.
   *
   * @param dirIndex the directory view index
   * @return a directory view
   */
  public StorageDirView getDirView(int dirIndex) {
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
}
