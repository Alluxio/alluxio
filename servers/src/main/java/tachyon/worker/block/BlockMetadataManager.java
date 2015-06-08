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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.allocator.NaiveAllocator;
import tachyon.worker.block.evictor.Evictor;
import tachyon.worker.block.evictor.NaiveEvictor;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageTier;

/**
 * Manages the metadata of all blocks in managed space. This information is used by the TieredBlockStore,
 * Allocator and Evictor.
 * <p>
 * This class is thread-safe and all operations on block metadata such as StorageTier, StorageDir
 * should go through this class.
 */
public class BlockMetadataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Allocator mAllocator;
  private final Evictor mEvictor;
  private long mAvailableSpace;
  private Map<Integer, StorageTier> mTiers;

  public BlockMetadataManager(TachyonConf tachyonConf) {
    // TODO: create Allocator according to tachyonConf.
    mAllocator = new NaiveAllocator(this);
    // TODO: create Evictor according to tachyonConf
    mEvictor = new NaiveEvictor(this);

    // Initialize storage tiers
    int totalTiers = tachyonConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
    mTiers = new HashMap<Integer, StorageTier>(totalTiers);
    for (int i = 0; i < totalTiers; i ++) {
      mTiers.put(i, new StorageTier(tachyonConf, i));
    }
  }

  public synchronized StorageTier getTier(int tierAlias) {
    return mTiers.get(tierAlias);
  }

  public synchronized Set<StorageTier> getTiers() {
    return new HashSet<StorageTier>(mTiers.values());
  }

  public synchronized long getAvailableSpace() {
    return mAvailableSpace;
  }

  /* Operations on metadata information */

  /**
   * Create the metadata of a new block in a specific tier. If there is no space for this block in
   * that tier, try to evict some blocks according to the eviction policy.
   *
   * @param userId the id of the user
   * @param blockId the id of the block
   * @param blockSize block size in bytes
   * @param tierHint which tier to create this block
   * @return the newly created block metadata or absent on creation failure.
   */
  public synchronized Optional<BlockMeta> createBlockMeta(long userId, long blockId,
      long blockSize, int tierHint) {
    Optional<BlockMeta> optionalBlock =
        mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
    if (!optionalBlock.isPresent()) {
      mEvictor.freeSpace(blockSize, tierHint);
      optionalBlock = mAllocator.allocateBlock(userId, blockId, blockSize, tierHint);
      if (!optionalBlock.isPresent()) {
        LOG.error("Cannot create block {}:", blockId);
        return Optional.absent();
      }
    }
    return optionalBlock;
  }

  /**
   * Get the metadata of a specific block.
   *
   * @param blockId the block ID
   * @return metadata of the block or absent
   */
  public synchronized Optional<BlockMeta> getBlockMeta(long blockId) {
    for (StorageTier tier : mTiers.values()) {
      Optional<BlockMeta> optionalBlock = tier.getBlockMeta(blockId);
      if (optionalBlock.isPresent()) {
        return optionalBlock;
      }
    }
    return Optional.absent();
  }

  /**
   * Add the metadata of a specific block to a storage tier.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param tierAlias alias of the tier
   * @return metadata of the block or absent
   */
  public synchronized Optional<BlockMeta> addBlockMetaInTier(long userId, long blockId,
      long blockSize, int tierAlias) {
    StorageTier tier = getTier(tierAlias);
    Preconditions.checkArgument(tier != null, "tierAlias must be valid: %s", tierAlias);
    return tier.addBlockMeta(userId, blockId, blockSize);
  }

  /**
   * Move the metadata of a specific block to another tier.
   *
   * @param blockId the block ID
   * @return the new block metadata if success, absent otherwise
   */
  public synchronized Optional<BlockMeta> moveBlockMeta(long userId, long blockId, int newTierAlias) {
    StorageTier tier = getTier(newTierAlias);
    if (tier == null) {
      LOG.error("tierAlias must be valid: {}", newTierAlias);
      return Optional.absent();
    }
    Optional<BlockMeta> optionalBlock = getBlockMeta(blockId);
    if (!optionalBlock.isPresent()) {
      LOG.error("No block found for block ID {}", blockId);
      return Optional.absent();
    }
    Preconditions.checkState(removeBlockMeta(blockId));
    return tier.addBlockMeta(userId, blockId, newTierAlias);
  }

  /**
   * Remove the metadata of a specific block.
   *
   * @param blockId the block ID
   * @return true if success, false otherwise
   */
  public synchronized boolean removeBlockMeta(long blockId) {
    for (StorageTier tier : mTiers.values()) {
      if (tier.removeBlockMeta(blockId)) {
        return true;
      }
    }
    return false;
  }
}
