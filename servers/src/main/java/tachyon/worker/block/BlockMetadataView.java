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

package tachyon.worker.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.StorageTierView;

/**
 * This class exposes a narrower view of {@link BlockMetadataManager} to Evictors and Allocators.
 */
public class BlockMetadataView {

  private final BlockMetadataManager mMetadataManager;
  private List<StorageTierView> mTierViews;
  private final List<Long> mPinnedBlocks = new ArrayList<Long>();
  private final List<Long> mdReadingBlocks = new ArrayList<Long>();

  public BlockMetadataView(BlockMetadataManager manager, ArrayList<Long> pinnedBlocks,
      ArrayList<Long> readingBlocks) throws IOException {
    mMetadataManager = Preconditions.checkNotNull(manager);
    mPinnedBlocks.addAll(Preconditions.checkNotNull(pinnedBlocks));
    mdReadingBlocks.addAll(Preconditions.checkNotNull(readingBlocks));

    // iteratively create all StorageTierViews and StorageDirViews
    for (StorageTier tier : manager.getTiers()) {
      StorageTierView tierView = new StorageTierView(tier, this);
      mTierViews.add(tierView);
    }
  }

  /**
   * Provide StorageTierView given tierAlias
   *
   * @param tierAlias the alias of this tierView
   * @return the StorageTierView object associated with the alias
   * @throws IOException if tierAlias is not found
   */
  public synchronized StorageTierView getTierView(int tierAlias) throws IOException {
    // TODO: can we ensure the returning tierview is same as
    // new StorageTierView(mMetadataManager.getTier(tierAlias)) ?
    return mTierViews.get(tierAlias);
  }

  /**
   *
   * @return the list of StorageTierViews
   */
  public synchronized List<StorageTierView> getTierViews() {
    return mTierViews;
  }

  /**
   *
   * @param tierAlias the alias of a tier
   * @return the list of StorageTierView
   * @throws IOException if tierAlias is not found
   */
  public synchronized List<StorageTierView> getTiersBelow(int tierAlias) throws IOException {
    // TODO: similar concern as in getTierView
    int level = getTierView(tierAlias).getTierViewLevel();
    return mTierViews.subList(level + 1, mTierViews.size());
  }

  /**
   * Redirecting to {@link BlockMetadataManager#getAvailableBytes(BlockStoreLocation)}
   *
   * @param location location the check available bytes
   * @return available bytes
   */
  public synchronized long getAvailableBytes(BlockStoreLocation location) throws IOException {
    return mMetadataManager.getAvailableBytes(location);
  }

  /**
   * Redirecting to {@link BlockMetadataManager#getBlockMeta(long)}
   *
   * @param blockId the block ID
   * @return metadata of the block or null
   * @throws IOException if no BlockMeta for this blockId is found
   */
  public synchronized BlockMeta getBlockMeta(long blockId) throws IOException {
    return getBlockMeta(blockId);
  }
}
