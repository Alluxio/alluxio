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

package tachyon.worker.block.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.worker.block.BlockMetadataManagerView;

/**
 * This class is a wrapper of {@link StorageTier} to provide more limited access
 */
public class StorageTierView {

  /** the StorageTier this view is derived from */
  private final StorageTier mTier;
  /** a list of StorageDirView under this StorageTierView */
  private List<StorageDirView> mDirViews = new ArrayList<StorageDirView>();
  /** the BlockMetadataView this StorageTierView is under */
  private final BlockMetadataManagerView mManagerView;

  /**
   * Create a StorageTierView using the actual StorageTier and the above BlockMetadataView
   *
   * @param tier which the tierView is constructed from
   * @param view, the BlockMetadataManagerView this tierView is associated with
   * @param StorageTierView constructed
   */
  public StorageTierView(StorageTier tier, BlockMetadataManagerView view) {
    mTier = Preconditions.checkNotNull(tier);
    mManagerView = Preconditions.checkNotNull(view);

    for (StorageDir dir : mTier.getStorageDirs()) {
      StorageDirView dirView = new StorageDirView(dir, this, view);
      mDirViews.add(dirView);
    }
  }

  /**
   * Get the list of StorageDirView under this TierView
   */
  public List<StorageDirView> getDirViews() {
    return mDirViews;
  }

  /**
   * Get a StorageDirView with a dirIndex
   *
   * @param direIndex of the dirView requested
   * @throws IOException if dirIndex is out of range
   */
  public StorageDirView getDirView(int dirIndex) throws IOException {
    return mDirViews.get(dirIndex);
  }

  /**
   * Get the alias for this tier
   */
  public int getTierViewAlias() {
    return mTier.getTierAlias();
  }

  /**
   * Get the level for this tier
   */
  public int getTierViewLevel() {
    return mTier.getTierLevel();
  }
}
