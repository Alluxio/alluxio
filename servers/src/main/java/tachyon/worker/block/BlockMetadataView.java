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
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * This class exposes a narrower view of {@link BlockMetadataManager} to Evictors and Allocators.
 */
public class BlockMetadataView {

  private final BlockMetadataManager mMetadataManager;

  public BlockMetadataView(BlockMetadataManager manager) {
    mMetadataManager = Preconditions.checkNotNull(manager);
  }

  /**
   * Redirecting to {@link BlockMetadataManager#getTier(int)}
   *
   * @param tierAlias the alias of this tier
   * @return the StorageTier object associated with the alias
   * @throws IOException if tierAlias is not found
   */
  public synchronized StorageTier getTier(int tierAlias) throws IOException {
    return mMetadataManager.getTier(tierAlias);
  }

  /**
   * Redirecting to {@link BlockMetadataManager#getTiers()}
   *
   * @return the list of StorageTiers
   */
  public synchronized List<StorageTier> getTiers() {
    return mMetadataManager.getTiers();
  }

  /**
   * Redirecting to {@link BlockMetadataManager#getTiersBelow(int)}
   *
   * @param tierAlias the alias of a tier
   * @return the list of StorageTier
   * @throws IOException if tierAlias is not found
   */
  public synchronized List<StorageTier> getTiersBelow(int tierAlias) throws IOException {
    return mMetadataManager.getTiersBelow(tierAlias);
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
