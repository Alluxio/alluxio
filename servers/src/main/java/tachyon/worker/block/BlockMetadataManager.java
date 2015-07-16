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
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.BlockMetaBase;
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
// TODO: consider how to better expose information to Evictor and Allocator.
public class BlockMetadataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** A list of managed StorageTier */
  private List<StorageTier> mTiers;
  /** A map from tier alias to StorageTier */
  private Map<Integer, StorageTier> mAliasToTiers;

  private BlockMetadataManager() {}

  private void initBlockMetadataManager(TachyonConf tachyonConf) throws IOException {
    // Initialize storage tiers
    int totalTiers = tachyonConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
    mAliasToTiers = new HashMap<Integer, StorageTier>(totalTiers);
    mTiers = new ArrayList<StorageTier>(totalTiers);
    for (int level = 0; level < totalTiers; level ++) {
      StorageTier tier = StorageTier.newStorageTier(tachyonConf, level);
      mTiers.add(tier);
      mAliasToTiers.put(tier.getTierAlias(), tier);
    }
  }

  public static BlockMetadataManager newBlockMetadataManager(TachyonConf tachyonConf)
      throws IOException {
    BlockMetadataManager ret = new BlockMetadataManager();
    ret.initBlockMetadataManager(tachyonConf);
    return ret;
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
   * Cleans up the meta data of the given temp block ids
   *
   * @param userId the ID of the client associated with the temp blocks
   * @param tempBlockIds the list of temporary block ids to be cleaned up, non temporary block
   *                     ids will be ignored.
   */
  public synchronized void cleanupUserTempBlocks(long userId, List<Long> tempBlockIds) {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dir.cleanupUserTempBlocks(userId, tempBlockIds);
      }
    }
  }

  /**
   * Gets the amount of available space of given location in bytes. Master queries the total number
   * of bytes available on each tier of the worker, and Evictor/Allocator often cares about the
   * bytes at a {@link StorageDir}.
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
   * Gets the metadata of a block given its blockId or throws IOException.
   *
   * @param blockId the block ID
   * @return metadata of the block
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
   * Returns the path of a block given its location, or null if the location is not a specific
   * StorageDir.
   *
   * @param blockId the ID of the block
   * @param location location of a particular StorageDir to store this block
   * @return the path of this block in this location
   * @throws IOException if location is not a specific StorageDir
   */
  public synchronized String getBlockPath(long blockId, BlockStoreLocation location)
      throws IOException {
    return BlockMetaBase.commitPath(getDir(location), blockId);
  }

  /**
   * Gets a summary of the meta data.
   *
   * @return the metadata of this block store
   */
  public synchronized BlockStoreMeta getBlockStoreMeta() {
    return new BlockStoreMeta(this);
  }


  /**
   * Gets the StorageDir given its location in the store.
   *
   * @param location Location of the dir
   * @return the StorageDir object
   * @throws IOException if location is not a specific dir or the location is invalid
   */
  public synchronized StorageDir getDir(BlockStoreLocation location) throws IOException {
    if (location.equals(BlockStoreLocation.anyTier())
        || location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      throw new IOException("Failed to get block path: " + location + " is not a specific dir.");
    }
    return getTier(location.tierAlias()).getDir(location.dir());
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
   * Gets the list of StorageTier below the tier with the given tierAlias.
   *
   * @param tierAlias the alias of a tier
   * @return the list of StorageTier
   * @throws IOException if tierAlias is not found
   */
  public synchronized List<StorageTier> getTiersBelow(int tierAlias) throws IOException {
    int level = getTier(tierAlias).getTierLevel();
    return mTiers.subList(level + 1, mTiers.size());
  }

  /**
   * Gets all the temporary blocks associated with a user, empty list is returned if the user has
   * no temporary blocks.
   *
   * @param userId the ID of the user
   * @return A list of temp blocks associated with the user
   */
  public synchronized List<TempBlockMeta> getUserTempBlocks(long userId) {
    List<TempBlockMeta> userTempBlocks = new ArrayList<TempBlockMeta>();
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        userTempBlocks.addAll(dir.getUserTempBlocks(userId));
      }
    }
    return userTempBlocks;
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
   * Moves the metadata of an existing block to another location or throws IOExceptions.
   *
   * @param blockMeta the meta data of the block to move
   * @param newLocation new location of the block
   * @return the new block metadata if success, absent otherwise
   * @throws IOException if this block is not found
   */
  public synchronized BlockMeta moveBlockMeta(BlockMeta blockMeta, BlockStoreLocation newLocation)
      throws IOException {
    // If existing location belongs to the target location, simply return the current block meta.
    BlockStoreLocation oldLocation = blockMeta.getBlockLocation();
    if (oldLocation.belongTo(newLocation)) {
      LOG.info("moveBlockMeta: moving {} to {} is a noop", oldLocation, newLocation);
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
      StorageDir dir = newTier.getDir(newLocation.dir());
      if (dir.getAvailableBytes() >= blockMeta.getBlockSize()) {
        newDir = dir;
      }
    }

    if (newDir == null) {
      throw new IOException("Failed to move BlockMeta: newLocation " + newLocation
          + " does not have enough space for " + blockMeta.getBlockSize() + " bytes");
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

}
