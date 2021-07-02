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

package alluxio.worker.block;

import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.StorageTierAllocatorView;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class exposes a narrower read-only view of block metadata to allocators.
 */
@NotThreadSafe
public class BlockMetadataAllocatorView extends BlockMetadataView {

  /**
   * Creates a new instance of {@link BlockMetadataAllocatorView}.
   *
   * @param manager which the view should be constructed from
   * @param useReservedSpace include reserved space in available bytes
   */
  public BlockMetadataAllocatorView(BlockMetadataManager manager, boolean useReservedSpace) {
    super(manager, useReservedSpace);
  }

  @Override
  public void initializeView() {
    // iteratively create all StorageTierViews and StorageDirViews
    for (StorageTier tier : mMetadataManager.getTiers()) {
      StorageTierAllocatorView tierView = new StorageTierAllocatorView(tier, mUseReservedSpace);
      mTierViews.add(tierView);
      mAliasToTierViews.put(tier.getTierAlias(), tierView);
    }
  }
}
