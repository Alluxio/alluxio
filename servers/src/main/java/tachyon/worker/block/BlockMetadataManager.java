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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * Manages the metadata of all blocks in managed space. This information is used by the
 * TieredBlockStore, Allocator and Evictor.
 * <p>
 * This class is thread-safe and all operations on block metadata such as StorageTier, StorageDir
 * should go through this class.
 */
public class BlockMetadataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** A list of managed StorageTier */
  private Map<Integer, StorageTier> mTiers;

  public BlockMetadataManager(TachyonConf tachyonConf) {
    // Initialize storage tiers
    int totalTiers = tachyonConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
    mTiers = new HashMap<Integer, StorageTier>(totalTiers);
    for (int i = 0; i < totalTiers; i ++) {
      int tierAlias = i + 1;
      mTiers.put(tierAlias, new StorageTier(tachyonConf, tierAlias));
    }
  }

  /**
   * Gets the StorageTier given its tierAlias.
   *
   * @param tierAlias the alias of this tier
   * @return the StorageTier object associated with the alias
   */
  public synchronized StorageTier getTier(int tierAlias) {
    return mTiers.get(tierAlias);
  }

  /**
   * Gets the list of StorageTier managed.
   *
   * @return the list of StorageTiers
   */
  public synchronized List<StorageTier> getTiers() {
    return new ArrayList<StorageTier>(mTiers.values());
  }

  /**
   * Gets the amount of available space in given location in bytes.
   *
   * @param location location the check available bytes
   * @return available bytes
   */
  public synchronized long getAvailableBytes(BlockStoreLocation location) {
    long spaceAvailable = 0;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : getTiers()) {
        spaceAvailable += tier.getAvailableBytes();
      }
      return spaceAvailable;
    }

    int tierAlias = location.tierAlias();
    StorageTier tier = getTier(tierAlias);
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      return tier.getAvailableBytes();
    }

    int dirIndex = location.dir();
    StorageDir dir = tier.getDir(dirIndex);
    return dir.getAvailableBytes();
  }

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block ID
   * @return true if the block is contained, false otherwise
   */
  public synchronized boolean hasBlockMeta(long blockId) {
    for (StorageTier tier : mTiers.values()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasBlockMeta(blockId)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Gets the metadata of a block given its blockId.
   *
   * @param blockId the block ID
   * @return metadata of the block or absent
   */
  public synchronized Optional<BlockMeta> getBlockMeta(long blockId) {
    for (StorageTier tier : mTiers.values()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasBlockMeta(blockId)) {
          return tier.getBlockMeta(blockId);
        }
      }
    }
    return Optional.absent();
  }

  /**
   * Moves the metadata of an existing block to another location.
   *
   * @param blockId the block ID
   * @return the new block metadata if success, absent otherwise
   */
  public synchronized Optional<BlockMeta> moveBlockMeta(long userId, long blockId,
      BlockStoreLocation newLocation) {
    // Check if the blockId is valid.
    BlockMeta block = getBlockMeta(blockId).orNull();
    if (block == null) {
      LOG.error("No block found for block ID {}", blockId);
      return Optional.absent();
    }

    // If move target can be any tier, then simply return the current block meta.
    if (newLocation.equals(BlockStoreLocation.anyTier())) {
      return Optional.of(block);
    }

    int newTierAlias = newLocation.tierAlias();
    StorageTier newTier = getTier(newTierAlias);
    StorageDir newDir = null;
    if (newLocation.equals(BlockStoreLocation.anyDirInTier(newTierAlias))) {
      for (StorageDir dir : newTier.getStorageDirs()) {
        if (dir.getAvailableBytes() > block.getBlockSize()) {
          newDir = dir;
        }
      }
    } else {
      newDir = newTier.getDir(newLocation.dir());
    }

    if (newDir == null) {
      return Optional.absent();
    }
    StorageDir oldDir = block.getParentDir();
    if (!oldDir.removeBlockMeta(block)) {
      return Optional.absent();
    }
    return newDir.addBlockMeta(block);
  }

  /**
   * Remove the metadata of a specific block.
   *
   * @param block the meta data of the block to remove
   * @return true if success, false otherwise
   */
  public synchronized boolean removeBlockMeta(BlockMeta block) {
    StorageDir dir = block.getParentDir();
    return dir.removeBlockMeta(block);
  }

  /**
   * Gets the metadata of a temp block.
   *
   * @param blockId the ID of the temp block
   * @return metadata of the block or absent
   */
  public synchronized Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    for (StorageTier tier : mTiers.values()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasTempBlockMeta(blockId)) {
          return dir.getTempBlockMeta(blockId);
        }
      }
    }
    return Optional.absent();
  }

  /**
   * Adds a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to add
   * @return true if success, false otherwise
   */
  public synchronized boolean addTempBlockMeta(TempBlockMeta tempBlockMeta) {
    StorageDir dir = tempBlockMeta.getParentDir();
    return dir.addTempBlockMeta(tempBlockMeta);
  }

  /**
   * Commits a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to commit
   * @return true if success, false otherwise
   */
  public synchronized boolean commitTempBlockMeta(TempBlockMeta tempBlockMeta) {
    BlockMeta block = new BlockMeta(tempBlockMeta);
    StorageDir dir = tempBlockMeta.getParentDir();
    return dir.removeTempBlockMeta(tempBlockMeta) && dir.addBlockMeta(block).isPresent();

  }

  /**
   * Aborts a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to add
   * @return true if success, false otherwise
   */
  public synchronized boolean abortTempBlockMeta(TempBlockMeta tempBlockMeta) {
    StorageDir dir = tempBlockMeta.getParentDir();
    return dir.removeTempBlockMeta(tempBlockMeta);
  }

  /**
   * Modifies the size of a temp block
   *
   * @param tempBlockMeta the temp block to modify
   * @param newSize new size in bytes
   */
  public synchronized void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize) {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.resizeTempBlockMeta(tempBlockMeta, newSize);
  }

    /**
   * Cleans up the temp blocks meta data created by the given user.
   *
   * @param userId the ID of the user
   */
  public synchronized void cleanupUser(long userId) {
    for (StorageTier tier : mTiers.values()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dir.cleanupUser(userId);
      }
    }
  }

  /**
   * Gets a summary of the meta data.
   *
   * @return the metadata of this block store
   */
  public synchronized BlockStoreMeta getBlockStoreMeta() {
    return new BlockStoreMeta(this);
  }
}
