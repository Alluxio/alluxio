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
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
  private List<StorageTier> mTiers;
  /** A map from tier alias to StorageTier */
  private Map<Integer, StorageTier> mAliasToTiers;

  public BlockMetadataManager(TachyonConf tachyonConf) {
    // Initialize storage tiers
    int totalTiers = tachyonConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
    mAliasToTiers = new HashMap<Integer, StorageTier>(totalTiers);
    mTiers = new ArrayList<StorageTier>(totalTiers);
    for (int level = 0; level < totalTiers; level ++) {
      StorageTier tier = new StorageTier(tachyonConf, level);
      mTiers.add(tier);
      mAliasToTiers.put(tier.getTierAlias(), tier);
    }
  }

  /**
   * Gets the StorageTier given its tierAlias.
   *
   * @param tierAlias the alias of this tier
   * @return the StorageTier object associated with the alias
   * @throws IOException if tierAlias is not found
   */
  public synchronized StorageTier getTier(int tierAlias) throws IOException {
    StorageTier tier = mAliasToTiers.get(tierAlias);
    if (tier == null) {
      throw new IOException("Cannot find tier with alias " + tierAlias);
    }
    return tier;
  }

  /**
   * Gets the list of StorageTier managed.
   *
   * @return the list of StorageTiers
   */
  public synchronized List<StorageTier> getTiers() {
    return mTiers;
  }

  /**
   * Gets the amount of available space in given location in bytes.
   *
   * @param location location the check available bytes
   * @return available bytes
   */
  public synchronized long getAvailableBytes(BlockStoreLocation location) throws IOException {
    long spaceAvailable = 0;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : mTiers) {
        spaceAvailable += tier.getAvailableBytes();
      }
      return spaceAvailable;
    }

    int tierAlias = location.tierAlias();
    StorageTier tier = getTier(tierAlias);
    // TODO: This should probably be max of the capacity bytes in the dirs?
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
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasBlockMeta(blockId)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Gets the metadata of a block given its blockId or throws IOException.
   *
   * @param blockId the block ID
   * @return metadata of the block or null
   * @throws IOException if no BlockMeta for this blockId is found
   */
  public synchronized BlockMeta getBlockMeta(long blockId) throws IOException {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasBlockMeta(blockId)) {
          return dir.getBlockMeta(blockId);
        }
      }
    }
    throw new IOException("Failed to get BlockMeta: blockId " + blockId + " not found");
  }

  /**
   * Moves the metadata of an existing block to another location or throws IOExceptions.
   *
   * @param blockMeta the meta data of the block to move
   * @param newLocation new location of the block
   * @return the new block metadata if success, absent otherwise
   * @throws IOException if this block is not found
   */
  public synchronized BlockMeta moveBlockMeta(BlockMeta blockMeta, BlockStoreLocation newLocation)
      throws IOException {
    // If move target can be any tier, then simply return the current block meta.
    if (newLocation.equals(BlockStoreLocation.anyTier())) {
      return blockMeta;
    }

    int newTierAlias = newLocation.tierAlias();
    StorageTier newTier = getTier(newTierAlias);
    StorageDir newDir = null;
    if (newLocation.equals(BlockStoreLocation.anyDirInTier(newTierAlias))) {
      for (StorageDir dir : newTier.getStorageDirs()) {
        if (dir.getAvailableBytes() >= blockMeta.getBlockSize()) {
          newDir = dir;
        }
      }
    } else {
      newDir = newTier.getDir(newLocation.dir());
    }

    if (newDir == null) {
      throw new IOException("Failed to move BlockMeta: newLocation " + newLocation
          + " has not enough space for " + blockMeta.getBlockSize() + " bytes");
    }
    StorageDir oldDir = blockMeta.getParentDir();
    oldDir.removeBlockMeta(blockMeta);
    BlockMeta newBlockMeta =
        new BlockMeta(blockMeta.getBlockId(), blockMeta.getBlockSize(), newDir);
    newDir.addBlockMeta(newBlockMeta);
    return newBlockMeta;
  }

  /**
   * Remove the metadata of a specific block.
   *
   * @param block the meta data of the block to remove
   * @throws IOException
   */
  public synchronized void removeBlockMeta(BlockMeta block) throws IOException {
    StorageDir dir = block.getParentDir();
    dir.removeBlockMeta(block);
  }

  /**
   * Checks if the storage has a given temp block.
   *
   * @param blockId the temp block ID
   * @return true if the block is contained, false otherwise
   */
  public synchronized boolean hasTempBlockMeta(long blockId) {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasTempBlockMeta(blockId)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Gets the metadata of a temp block.
   *
   * @param blockId the ID of the temp block
   * @return metadata of the block or null
   * @throws IOException
   */
  public synchronized TempBlockMeta getTempBlockMeta(long blockId) throws IOException {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasTempBlockMeta(blockId)) {
          return dir.getTempBlockMeta(blockId);
        }
      }
    }
    throw new IOException("Failed to get TempBlockMeta: temp blockId " + blockId + " not found");
  }

  /**
   * Adds a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to add
   */
  public synchronized void addTempBlockMeta(TempBlockMeta tempBlockMeta) throws IOException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.addTempBlockMeta(tempBlockMeta);
  }

  /**
   * Commits a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to commit
   * @throws IOException
   */
  public synchronized void commitTempBlockMeta(TempBlockMeta tempBlockMeta) throws IOException {
    BlockMeta block = new BlockMeta(Preconditions.checkNotNull(tempBlockMeta));
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.removeTempBlockMeta(tempBlockMeta);
    dir.addBlockMeta(block);
  }

  /**
   * Aborts a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to add
   * @throws IOException
   */
  public synchronized void abortTempBlockMeta(TempBlockMeta tempBlockMeta) throws IOException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.removeTempBlockMeta(tempBlockMeta);
  }

  /**
   * Modifies the size of a temp block
   *
   * @param tempBlockMeta the temp block to modify
   * @param newSize new size in bytes
   */
  public synchronized void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize)
      throws IOException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.resizeTempBlockMeta(tempBlockMeta, newSize);
  }

  /**
   * Cleans up the meta data of temp blocks created by the given user.
   *
   * @param userId the ID of the user
   * @return A list of temp blocks created by the user in this block store
   */
  public synchronized List<TempBlockMeta> cleanupUser(long userId) {
    List<TempBlockMeta> ret = new ArrayList<TempBlockMeta>();
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        List<TempBlockMeta> blocksToRemove = dir.cleanupUser(userId);
        if (blocksToRemove != null) {
          for (TempBlockMeta block : blocksToRemove) {
            ret.add(block);
          }
        }
      }
    }
    return ret;
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
