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
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.BlockMetaBase;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * Manages the metadata of all blocks in managed space. This information is used by the
 * TieredBlockStore, Allocator and Evictor.
 * <p>
 * This class is NOT thread-safe. All operations on block metadata such as StorageTier, StorageDir
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

  /**
   * Factory method to create {@link BlockMetadataManager}.
   *
   * @return the new created BlockMetadataManager
   */
  public static BlockMetadataManager newBlockMetadataManager() {
    BlockMetadataManager ret = new BlockMetadataManager();
    try {
      ret.initBlockMetadataManager();
      // caller of newBlockMetadataManager should not be forced to catch and handle these exceptions
      // since it is the responsibility of BlockMetadataManager.
    } catch (AlreadyExistsException aee) {
      throw new RuntimeException(aee);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } catch (OutOfSpaceException ooe) {
      throw new RuntimeException(ooe);
    }
    return ret;
  }

  private void initBlockMetadataManager() throws AlreadyExistsException, IOException,
      OutOfSpaceException {
    // Initialize storage tiers
    int totalTiers = WorkerContext.getConf().getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL);
    mAliasToTiers = new HashMap<Integer, StorageTier>(totalTiers);
    mTiers = new ArrayList<StorageTier>(totalTiers);
    for (int level = 0; level < totalTiers; level ++) {
      StorageTier tier = StorageTier.newStorageTier(level);
      mTiers.add(tier);
      mAliasToTiers.put(tier.getTierAlias(), tier);
    }
  }

  /**
   * Aborts a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to add
   * @throws NotFoundException when block can not be found
   */
  public void abortTempBlockMeta(TempBlockMeta tempBlockMeta) throws NotFoundException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.removeTempBlockMeta(tempBlockMeta);
  }

  /**
   * Adds a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to add
   * @throws OutOfSpaceException when no more space left to hold the block
   * @throws AlreadyExistsException when the block already exists
   */
  public void addTempBlockMeta(TempBlockMeta tempBlockMeta) throws OutOfSpaceException,
      AlreadyExistsException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.addTempBlockMeta(tempBlockMeta);
  }

  /**
   * Commits a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to commit
   * @throws OutOfSpaceException when no more space left to hold the block
   * @throws AlreadyExistsException when the block already exists in committed blocks
   * @throws NotFoundException when temp block can not be found
   */
  public void commitTempBlockMeta(TempBlockMeta tempBlockMeta) throws OutOfSpaceException,
      AlreadyExistsException, NotFoundException {
    BlockMeta block = new BlockMeta(Preconditions.checkNotNull(tempBlockMeta));
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.removeTempBlockMeta(tempBlockMeta);
    dir.addBlockMeta(block);
  }

  /**
   * Cleans up the meta data of the given temp block ids
   *
   * @param sessionId the ID of the client associated with the temp blocks
   * @param tempBlockIds the list of temporary block ids to be cleaned up, non temporary block ids
   *        will be ignored.
   */
  @Deprecated
  public void cleanupSessionTempBlocks(long sessionId, List<Long> tempBlockIds) {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dir.cleanupSessionTempBlocks(sessionId, tempBlockIds);
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
   * @throws IllegalArgumentException when location does not belong to tiered storage
   */
  public long getAvailableBytes(BlockStoreLocation location) {
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
   * Gets the metadata of a block given its blockId.
   *
   * @param blockId the block ID
   * @return metadata of the block
   * @throws NotFoundException if no BlockMeta for this blockId is found
   */
  public BlockMeta getBlockMeta(long blockId) throws NotFoundException {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasBlockMeta(blockId)) {
          return dir.getBlockMeta(blockId);
        }
      }
    }
    throw new NotFoundException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
  }

  /**
   * Returns the path of a block given its location, or null if the location is not a specific
   * StorageDir.
   *
   * @param blockId the ID of the block
   * @param location location of a particular StorageDir to store this block
   * @return the path of this block in this location
   * @throws IllegalArgumentException if location is not a specific StorageDir
   */
  public String getBlockPath(long blockId, BlockStoreLocation location) {
    return BlockMetaBase.commitPath(getDir(location), blockId);
  }

  /**
   * Gets a summary of the meta data.
   *
   * @return the metadata of this block store
   */
  public BlockStoreMeta getBlockStoreMeta() {
    return new BlockStoreMeta(this);
  }


  /**
   * Gets the StorageDir given its location in the store.
   *
   * @param location Location of the dir
   * @return the StorageDir object
   * @throws IllegalArgumentException if location is not a specific dir or the location is invalid
   */
  public StorageDir getDir(BlockStoreLocation location) {
    if (location.equals(BlockStoreLocation.anyTier())
        || location.equals(BlockStoreLocation.anyDirInTier(location.tierAlias()))) {
      throw new IllegalArgumentException(
          ExceptionMessage.GET_DIR_FROM_NON_SPECIFIC_LOCATION.getMessage(location));
    }
    return getTier(location.tierAlias()).getDir(location.dir());
  }

  /**
   * Gets the metadata of a temp block.
   *
   * @param blockId the ID of the temp block
   * @return metadata of the block or null
   * @throws NotFoundException when blockId can not be found
   */
  public TempBlockMeta getTempBlockMeta(long blockId) throws NotFoundException {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasTempBlockMeta(blockId)) {
          return dir.getTempBlockMeta(blockId);
        }
      }
    }
    throw new NotFoundException(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND, blockId);
  }

  /**
   * Gets the StorageTier given its tierAlias.
   *
   * @param tierAlias the alias of this tier
   * @return the StorageTier object associated with the alias
   * @throws IllegalArgumentException if tierAlias is not found
   */
  public StorageTier getTier(int tierAlias) {
    StorageTier tier = mAliasToTiers.get(tierAlias);
    if (tier == null) {
      throw new IllegalArgumentException(
          ExceptionMessage.TIER_ALIAS_NOT_FOUND.getMessage(tierAlias));
    }
    return tier;
  }

  /**
   * Gets the list of StorageTier managed.
   *
   * @return the list of StorageTiers
   */
  public List<StorageTier> getTiers() {
    return mTiers;
  }

  /**
   * Gets the list of StorageTier below the tier with the given tierAlias.
   *
   * @param tierAlias the alias of a tier
   * @return the list of StorageTier
   * @throws IllegalArgumentException if tierAlias is not found
   */
  public List<StorageTier> getTiersBelow(int tierAlias) {
    int level = getTier(tierAlias).getTierLevel();
    return mTiers.subList(level + 1, mTiers.size());
  }

  /**
   * Gets all the temporary blocks associated with a session, empty list is returned if the session
   * has no temporary blocks.
   *
   * @param sessionId the ID of the session
   * @return A list of temp blocks associated with the session
   */
  public List<TempBlockMeta> getSessionTempBlocks(long sessionId) {
    List<TempBlockMeta> sessionTempBlocks = new ArrayList<TempBlockMeta>();
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        sessionTempBlocks.addAll(dir.getSessionTempBlocks(sessionId));
      }
    }
    return sessionTempBlocks;
  }

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block ID
   * @return true if the block is contained, false otherwise
   */
  public boolean hasBlockMeta(long blockId) {
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
  public boolean hasTempBlockMeta(long blockId) {
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
   * Moves an existing block to another location currently hold by a temp block.
   *
   * @param blockMeta the meta data of the block to move
   * @param tempBlockMeta a placeholder in the destination directory
   * @return the new block metadata if success, absent otherwise
   * @throws NotFoundException when the block to move is not found
   * @throws AlreadyExistsException when the block to move already exists in the destination
   * @throws OutOfSpaceException when destination have no extra space to hold the block to move
   */
  public BlockMeta moveBlockMeta(BlockMeta blockMeta, TempBlockMeta tempBlockMeta)
      throws NotFoundException, OutOfSpaceException, AlreadyExistsException {
    StorageDir srcDir = blockMeta.getParentDir();
    StorageDir dstDir = tempBlockMeta.getParentDir();
    srcDir.removeBlockMeta(blockMeta);
    BlockMeta newBlockMeta =
        new BlockMeta(blockMeta.getBlockId(), blockMeta.getBlockSize(), dstDir);
    dstDir.removeTempBlockMeta(tempBlockMeta);
    dstDir.addBlockMeta(newBlockMeta);
    return newBlockMeta;
  }

  /**
   * Moves the metadata of an existing block to another location or throws IOExceptions.
   *
   * @param blockMeta the meta data of the block to move
   * @param newLocation new location of the block
   * @return the new block metadata if success, absent otherwise
   * @throws IllegalArgumentException when the newLocation is not in the tiered storage
   * @throws NotFoundException when the block to move is not found
   * @throws AlreadyExistsException when the block to move already exists in the destination
   * @throws OutOfSpaceException when destination have no extra space to hold the block to move
   */
  @Deprecated
  public BlockMeta moveBlockMeta(BlockMeta blockMeta, BlockStoreLocation newLocation)
      throws NotFoundException, AlreadyExistsException, OutOfSpaceException {
    // If existing location belongs to the target location, simply return the current block meta.
    BlockStoreLocation oldLocation = blockMeta.getBlockLocation();
    if (oldLocation.belongTo(newLocation)) {
      LOG.info("moveBlockMeta: moving {} to {} is a noop", oldLocation, newLocation);
      return blockMeta;
    }

    long blockSize = blockMeta.getBlockSize();
    int newTierAlias = newLocation.tierAlias();
    StorageTier newTier = getTier(newTierAlias);
    StorageDir newDir = null;
    if (newLocation.equals(BlockStoreLocation.anyDirInTier(newTierAlias))) {
      for (StorageDir dir : newTier.getStorageDirs()) {
        if (dir.getAvailableBytes() >= blockSize) {
          newDir = dir;
          break;
        }
      }
    } else {
      StorageDir dir = newTier.getDir(newLocation.dir());
      if (dir.getAvailableBytes() >= blockSize) {
        newDir = dir;
      }
    }

    if (newDir == null) {
      throw new OutOfSpaceException("Failed to move BlockMeta: newLocation " + newLocation
          + " does not have enough space for " + blockSize + " bytes");
    }
    StorageDir oldDir = blockMeta.getParentDir();
    oldDir.removeBlockMeta(blockMeta);
    BlockMeta newBlockMeta = new BlockMeta(blockMeta.getBlockId(), blockSize, newDir);
    newDir.addBlockMeta(newBlockMeta);
    return newBlockMeta;
  }

  /**
   * Remove the metadata of a specific block.
   *
   * @param block the meta data of the block to remove
   * @throws NotFoundException when block is not found
   */
  public void removeBlockMeta(BlockMeta block) throws NotFoundException {
    StorageDir dir = block.getParentDir();
    dir.removeBlockMeta(block);
  }

  /**
   * Modifies the size of a temp block
   *
   * @param tempBlockMeta the temp block to modify
   * @param newSize new size in bytes
   * @throws InvalidStateException when newSize is smaller than current size
   */
  public void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize)
      throws InvalidStateException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.resizeTempBlockMeta(tempBlockMeta, newSize);
  }

}
