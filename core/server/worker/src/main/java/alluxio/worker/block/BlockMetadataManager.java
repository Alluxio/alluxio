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

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.evictor.Evictor;
import alluxio.worker.block.meta.AbstractBlockMeta;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manages the metadata of all blocks in managed space. This information is used by the
 * {@link TieredBlockStore}, {@link Allocator} and {@link Evictor}.
 * <p>
 * All operations on block metadata such as {@link StorageTier}, {@link StorageDir} should go
 * through this class.
 */
@NotThreadSafe
// TODO(bin): consider how to better expose information to Evictor and Allocator.
public final class BlockMetadataManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMetadataManager.class);

  /** A list of managed {@link StorageTier}, in order from lowest tier ordinal to greatest. */
  private final List<StorageTier> mTiers;

  /** A map from tier alias to {@link StorageTier}. */
  private final Map<String, StorageTier> mAliasToTiers;

  private final StorageTierAssoc mStorageTierAssoc;

  private BlockMetadataManager() {
    try {
      mStorageTierAssoc = new WorkerStorageTierAssoc();
      mAliasToTiers = new HashMap<>(mStorageTierAssoc.size());
      mTiers = new ArrayList<>(mStorageTierAssoc.size());
      for (int tierOrdinal = 0; tierOrdinal < mStorageTierAssoc.size(); tierOrdinal++) {
        StorageTier tier = StorageTier.newStorageTier(mStorageTierAssoc.getAlias(tierOrdinal));
        mTiers.add(tier);
        mAliasToTiers.put(tier.getTierAlias(), tier);
      }
    } catch (BlockAlreadyExistsException | IOException | WorkerOutOfSpaceException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a new instance of {@link BlockMetadataManager}.
   *
   * @return a {@link BlockMetadataManager} instance
   */
  public static BlockMetadataManager createBlockMetadataManager() {
    return new BlockMetadataManager();
  }

  /**
   * Aborts a temp block.
   *
   * @param tempBlockMeta the metadata of the temp block to add
   * @throws BlockDoesNotExistException when block can not be found
   */
  public void abortTempBlockMeta(TempBlockMeta tempBlockMeta) throws BlockDoesNotExistException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.removeTempBlockMeta(tempBlockMeta);
  }

  /**
   * Adds a temp block.
   *
   * @param tempBlockMeta the metadata of the temp block to add
   * @throws WorkerOutOfSpaceException when no more space left to hold the block
   * @throws BlockAlreadyExistsException when the block already exists
   */
  public void addTempBlockMeta(TempBlockMeta tempBlockMeta)
      throws WorkerOutOfSpaceException, BlockAlreadyExistsException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.addTempBlockMeta(tempBlockMeta);
  }

  /**
   * Commits a temp block.
   *
   * @param tempBlockMeta the metadata of the temp block to commit
   * @throws WorkerOutOfSpaceException when no more space left to hold the block
   * @throws BlockAlreadyExistsException when the block already exists in committed blocks
   * @throws BlockDoesNotExistException when temp block can not be found
   */
  public void commitTempBlockMeta(TempBlockMeta tempBlockMeta)
      throws WorkerOutOfSpaceException, BlockAlreadyExistsException, BlockDoesNotExistException {
    long blockId = tempBlockMeta.getBlockId();
    if (hasBlockMeta(blockId)) {
      BlockMeta blockMeta = getBlockMeta(blockId);
      throw new BlockAlreadyExistsException(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(blockId,
          blockMeta.getBlockLocation().tierAlias()));
    }
    BlockMeta block = new BlockMeta(Preconditions.checkNotNull(tempBlockMeta));
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.removeTempBlockMeta(tempBlockMeta);
    dir.addBlockMeta(block);
  }

  /**
   * Cleans up the metadata of the given temp block ids.
   *
   * @param sessionId the id of the client associated with the temp blocks
   * @param tempBlockIds the list of temporary block ids to be cleaned up, non temporary block ids
   *        will be ignored.
   * @deprecated As of version 0.8.
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
   * bytes at a {@link StorageDir}. Throws an {@link IllegalArgumentException} when the location
   * does not belong to the tiered storage.
   *
   * @param location location the check available bytes
   * @return available bytes
   */
  public long getAvailableBytes(BlockStoreLocation location) {
    long spaceAvailable = 0;

    if (location.equals(BlockStoreLocation.anyTier())) {
      for (StorageTier tier : mTiers) {
        spaceAvailable += tier.getAvailableBytes();
      }
      return spaceAvailable;
    }

    String tierAlias = location.tierAlias();
    StorageTier tier = getTier(tierAlias);
    // TODO(calvin): This should probably be max of the capacity bytes in the dirs?
    if (location.equals(BlockStoreLocation.anyDirInTier(tierAlias))) {
      return tier.getAvailableBytes();
    }

    int dirIndex = location.dir();
    StorageDir dir = tier.getDir(dirIndex);
    return dir.getAvailableBytes();
  }

  /**
   * Gets the metadata of a block given its block id.
   *
   * @param blockId the block id
   * @return metadata of the block
   * @throws BlockDoesNotExistException if no BlockMeta for this block id is found
   */
  public BlockMeta getBlockMeta(long blockId) throws BlockDoesNotExistException {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasBlockMeta(blockId)) {
          return dir.getBlockMeta(blockId);
        }
      }
    }
    throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
  }

  /**
   * Returns the path of a block given its location, or null if the location is not a specific
   * {@link StorageDir}. Throws an {@link IllegalArgumentException} if the location is not a
   * specific {@link StorageDir}.
   *
   * @param blockId the id of the block
   * @param location location of a particular {@link StorageDir} to store this block
   * @return the path of this block in this location
   */
  public String getBlockPath(long blockId, BlockStoreLocation location) {
    return AbstractBlockMeta.commitPath(getDir(location), blockId);
  }

  /**
   * Gets a summary of the metadata.
   *
   * @return the metadata of this block store
   */
  public BlockStoreMeta getBlockStoreMeta() {
    return BlockStoreMeta.Factory.create(this);
  }

  /**
   * Gets a full summary of block store metadata. This is an expensive operation.
   *
   * @return the full metadata of this block store
   */
  public BlockStoreMeta getBlockStoreMetaFull() {
    return BlockStoreMeta.Factory.createFull(this);
  }

  /**
   * Gets the {@link StorageDir} given its location in the store. Throws an
   * {@link IllegalArgumentException} if the location is not a specific dir or the location is
   * invalid.
   *
   * @param location Location of the dir
   * @return the {@link StorageDir} object
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
   * @param blockId the id of the temp block
   * @return metadata of the block
   * @throws BlockDoesNotExistException when block id can not be found
   */
  public TempBlockMeta getTempBlockMeta(long blockId) throws BlockDoesNotExistException {
    TempBlockMeta blockMeta = getTempBlockMetaOrNull(blockId);
    if (blockMeta == null) {
      throw new BlockDoesNotExistException(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND, blockId);
    }
    return blockMeta;
  }

  /**
   * Gets the metadata of a temp block.
   *
   * @param blockId the id of the temp block
   * @return metadata of the block or null
   */
  @Nullable
  public TempBlockMeta getTempBlockMetaOrNull(long blockId) {
    for (StorageTier tier : mTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.hasTempBlockMeta(blockId)) {
          return dir.getTempBlockMeta(blockId);
        }
      }
    }
    return null;
  }

  /**
   * Gets the {@link StorageTier} given its tierAlias. Throws an {@link IllegalArgumentException} if
   * the tierAlias is not found.
   *
   * @param tierAlias the alias of this tier
   * @return the {@link StorageTier} object associated with the alias
   */
  public StorageTier getTier(String tierAlias) {
    StorageTier tier = mAliasToTiers.get(tierAlias);
    if (tier == null) {
      throw new IllegalArgumentException(
          ExceptionMessage.TIER_ALIAS_NOT_FOUND.getMessage(tierAlias));
    }
    return tier;
  }

  /**
   * Gets the list of {@link StorageTier} managed.
   *
   * @return the list of {@link StorageTier}s
   */
  public List<StorageTier> getTiers() {
    return mTiers;
  }

  /**
   * Gets the list of {@link StorageTier} below the tier with the given tierAlias. Throws an
   * {@link IllegalArgumentException} if the tierAlias is not found.
   *
   * @param tierAlias the alias of a tier
   * @return the list of {@link StorageTier}
   */
  public List<StorageTier> getTiersBelow(String tierAlias) {
    int ordinal = getTier(tierAlias).getTierOrdinal();
    return mTiers.subList(ordinal + 1, mTiers.size());
  }

  /**
   * Gets all the temporary blocks associated with a session, empty list is returned if the session
   * has no temporary blocks.
   *
   * @param sessionId the id of the session
   * @return A list of temp blocks associated with the session
   */
  public List<TempBlockMeta> getSessionTempBlocks(long sessionId) {
    List<TempBlockMeta> sessionTempBlocks = new ArrayList<>();
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
   * @param blockId the block id
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
   * @param blockId the temp block id
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
   * @param blockMeta the metadata of the block to move
   * @param tempBlockMeta a placeholder in the destination directory
   * @return the new block metadata if success, absent otherwise
   * @throws BlockDoesNotExistException when the block to move is not found
   * @throws BlockAlreadyExistsException when the block to move already exists in the destination
   * @throws WorkerOutOfSpaceException when destination have no extra space to hold the block to
   *         move
   */
  public BlockMeta moveBlockMeta(BlockMeta blockMeta, TempBlockMeta tempBlockMeta)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, BlockAlreadyExistsException {
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
   * Moves the metadata of an existing block to another location or throws IOExceptions. Throws an
   * {@link IllegalArgumentException} if the newLocation is not in the tiered storage.
   *
   * @param blockMeta the metadata of the block to move
   * @param newLocation new location of the block
   * @return the new block metadata if success, absent otherwise
   * @throws BlockDoesNotExistException when the block to move is not found
   * @throws BlockAlreadyExistsException when the block to move already exists in the destination
   * @throws WorkerOutOfSpaceException when destination have no extra space to hold the block to
   *         move
   * @deprecated As of version 0.8. Use {@link #moveBlockMeta(BlockMeta, TempBlockMeta)} instead.
   */
  @Deprecated
  public BlockMeta moveBlockMeta(BlockMeta blockMeta, BlockStoreLocation newLocation)
      throws BlockDoesNotExistException, BlockAlreadyExistsException,
             WorkerOutOfSpaceException {
    // If existing location belongs to the target location, simply return the current block meta.
    BlockStoreLocation oldLocation = blockMeta.getBlockLocation();
    if (oldLocation.belongsTo(newLocation)) {
      LOG.info("moveBlockMeta: moving {} to {} is a noop", oldLocation, newLocation);
      return blockMeta;
    }

    long blockSize = blockMeta.getBlockSize();
    String newTierAlias = newLocation.tierAlias();
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
      throw new WorkerOutOfSpaceException("Failed to move BlockMeta: newLocation " + newLocation
          + " does not have enough space for " + blockSize + " bytes");
    }
    StorageDir oldDir = blockMeta.getParentDir();
    oldDir.removeBlockMeta(blockMeta);
    BlockMeta newBlockMeta = new BlockMeta(blockMeta.getBlockId(), blockSize, newDir);
    newDir.addBlockMeta(newBlockMeta);
    return newBlockMeta;
  }

  /**
   * Removes the metadata of a specific block.
   *
   * @param block the metadata of the block to remove
   * @throws BlockDoesNotExistException when block is not found
   */
  public void removeBlockMeta(BlockMeta block) throws BlockDoesNotExistException {
    StorageDir dir = block.getParentDir();
    dir.removeBlockMeta(block);
  }

  /**
   * Modifies the size of a temp block.
   *
   * @param tempBlockMeta the temp block to modify
   * @param newSize new size in bytes
   * @throws InvalidWorkerStateException when newSize is smaller than current size
   */
  public void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize)
      throws InvalidWorkerStateException {
    StorageDir dir = tempBlockMeta.getParentDir();
    dir.resizeTempBlockMeta(tempBlockMeta, newSize);
  }

  /**
   * @return the storage tier mapping
   */
  public StorageTierAssoc getStorageTierAssoc() {
    return mStorageTierAssoc;
  }
}
