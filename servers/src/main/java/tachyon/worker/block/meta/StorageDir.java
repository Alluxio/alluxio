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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.util.io.FileUtils;
import tachyon.worker.block.BlockStoreLocation;

/**
 * Represents a directory in a storage tier. It has a fixed capacity allocated to it on
 * instantiation. It contains the set of blocks currently in the storage directory.
 * <p>
 * This class does not guarantee thread safety.
 */
public final class StorageDir {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final long mCapacityBytes;
  /** A map from block ID to block meta data */
  private Map<Long, BlockMeta> mBlockIdToBlockMap;
  /** A map from block ID to temp block meta data */
  private Map<Long, TempBlockMeta> mBlockIdToTempBlockMap;
  /** A map from user ID to the set of temp blocks created by this user */
  private Map<Long, Set<Long>> mUserIdToTempBlockIdsMap;
  private AtomicLong mAvailableBytes;
  private AtomicLong mCommittedBytes;
  private String mDirPath;
  private int mDirIndex;
  private StorageTier mTier;

  private StorageDir(StorageTier tier, int dirIndex, long capacityBytes, String dirPath) {
    mTier = Preconditions.checkNotNull(tier);
    mDirIndex = dirIndex;
    mCapacityBytes = capacityBytes;
    mAvailableBytes = new AtomicLong(capacityBytes);
    mCommittedBytes = new AtomicLong(0);
    mDirPath = dirPath;
    mBlockIdToBlockMap = new HashMap<Long, BlockMeta>(200);
    mBlockIdToTempBlockMap = new HashMap<Long, TempBlockMeta>(200);
    mUserIdToTempBlockIdsMap = new HashMap<Long, Set<Long>>(200);
  }

  /**
   * Factory method to create {@link StorageDir}.
   *
   * It will load meta data of existing committed blocks in the dirPath specified. Only files with
   * directory depth 1 under dirPath and whose file name can be parsed into {@code long} will be
   * considered as existing committed blocks, these files will be preserved, others files or
   * directories will be deleted.
   *
   * @param tier the {@link StorageTier} this dir belongs to
   * @param dirIndex the index of this dir in its tier
   * @param capacityBytes the initial capacity of this dir, can not be modified later
   * @param dirPath filesystem path of this dir for actual storage
   * @return the new created StorageDir
   * @throws AlreadyExistsException when meta data of existing committed blocks already exists
   * @throws IOException if the storage directory cannot be created with the appropriate permissions
   * @throws OutOfSpaceException when meta data can not be added due to limited left space
   */
  public static StorageDir newStorageDir(StorageTier tier, int dirIndex, long capacityBytes,
      String dirPath) throws AlreadyExistsException, IOException, OutOfSpaceException {
    StorageDir dir = new StorageDir(tier, dirIndex, capacityBytes, dirPath);
    dir.initializeMeta();
    return dir;
  }

  /**
   * Initializes meta data for existing blocks in this StorageDir.
   *
   * Only paths satisfying the contract defined in {@link BlockMetaBase#commitPath} are legal,
   * should be in format like {dir}/{blockId}. other paths will be deleted.
   *
   * @throws AlreadyExistsException when meta data of existing committed blocks already exists
   * @throws IOException if the storage directory cannot be created with the appropriate permissions
   * @throws OutOfSpaceException when meta data can not be added due to limited left space
   */
  private void initializeMeta() throws AlreadyExistsException, IOException, OutOfSpaceException {
    // Create the storage directory path
    FileUtils.createStorageDirPath(mDirPath);

    File dir = new File(mDirPath);
    File[] paths = dir.listFiles();
    if (paths == null) {
      return;
    }
    for (File path : paths) {
      if (!path.isFile()) {
        LOG.error("{} in StorageDir is not a file", path.getAbsolutePath());
        try {
          // TODO: Resolve this conflict in class names
          org.apache.commons.io.FileUtils.deleteDirectory(path);
        } catch (IOException ioe) {
          LOG.error("can not delete directory {}: {}", path.getAbsolutePath(), ioe);
        }
      } else {
        try {
          long blockId = Long.valueOf(path.getName());
          addBlockMeta(new BlockMeta(blockId, path.length(), this));
        } catch (NumberFormatException nfe) {
          LOG.error("filename of {} in StorageDir can not be parsed into long",
              path.getAbsolutePath());
          if (path.delete()) {
            LOG.warn("file {} has been deleted", path.getAbsolutePath());
          } else {
            LOG.error("can not delete file {}", path.getAbsolutePath());
          }
        }
      }
    }
  }

  /**
   * Gets the total capacity of this StorageDir in bytes, which is a constant once this StorageDir
   * has been initialized.
   *
   * @return the total capacity of this StorageDir in bytes.
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * Gets the total available capacity of this StorageDir in bytes. This value equals the total
   * capacity of this StorageDir, minus the used bytes by committed blocks and temp blocks.
   *
   * @return available capacity in bytes
   */
  public long getAvailableBytes() {
    return mAvailableBytes.get();
  }

  /**
   * Gets the total size of committed blocks in this StorageDir in bytes.
   *
   * @return number of committed bytes.
   */
  public long getCommittedBytes() {
    return mCommittedBytes.get();
  }

  public String getDirPath() {
    return mDirPath;
  }

  /**
   * Returns the StorageTier containing this StorageDir.
   *
   * @return StorageTier
   */
  public StorageTier getParentTier() {
    return mTier;
  }

  /**
   * Returns the zero-based index of this dir in its parent StorageTier.
   *
   * @return index
   */
  public int getDirIndex() {
    return mDirIndex;
  }

  // TODO: deprecate this method.
  public long getStorageDirId() {
    int level = mTier.getTierLevel();
    int storageLevelAliasValue = mTier.getTierAlias();
    return StorageDirId.getStorageDirId(level, storageLevelAliasValue, mDirIndex);
  }

  /**
   * Returns the list of block IDs in this dir.
   *
   * @return a list of block IDs
   */
  public List<Long> getBlockIds() {
    return new ArrayList<Long>(mBlockIdToBlockMap.keySet());
  }

  /**
   * Returns the list of blocks stored in this dir.
   *
   * @return a list of blocks
   */
  public List<BlockMeta> getBlocks() {
    return new ArrayList<BlockMeta>(mBlockIdToBlockMap.values());
  }

  /**
   * Checks if a block is in this storage dir.
   *
   * @param blockId the block ID
   * @return true if the block is in this storage dir, false otherwise
   */
  public boolean hasBlockMeta(long blockId) {
    return mBlockIdToBlockMap.containsKey(blockId);
  }

  /**
   * Checks if a temp block is in this storage dir.
   *
   * @param blockId the block ID
   * @return true if the block is in this storage dir, false otherwise
   */
  public boolean hasTempBlockMeta(long blockId) {
    return mBlockIdToTempBlockMap.containsKey(blockId);
  }

  /**
   * Gets the BlockMeta from this storage dir by its block ID.
   *
   * @param blockId the block ID
   * @return BlockMeta of the given block or null
   * @throws NotFoundException if no block is found
   */
  public BlockMeta getBlockMeta(long blockId) throws NotFoundException {
    BlockMeta blockMeta = mBlockIdToBlockMap.get(blockId);
    if (blockMeta == null) {
      throw new NotFoundException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
    }
    return blockMeta;
  }

  /**
   * Gets the BlockMeta from this storage dir by its block ID.
   *
   * @param blockId the block ID
   * @return TempBlockMeta of the given block or null
   * @throws NotFoundException if no temp block is found
   */
  public TempBlockMeta getTempBlockMeta(long blockId) throws NotFoundException {
    TempBlockMeta tempBlockMeta = mBlockIdToTempBlockMap.get(blockId);
    if (tempBlockMeta == null) {
      throw new NotFoundException(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND, blockId);
    }
    return tempBlockMeta;
  }

  /**
   * Adds the metadata of a new block into this storage dir.
   *
   * @param blockMeta the meta data of the block
   * @throws AlreadyExistsException if blockId already exists
   * @throws OutOfSpaceException when not enough space to hold block
   */
  public void addBlockMeta(BlockMeta blockMeta) throws OutOfSpaceException, AlreadyExistsException {
    Preconditions.checkNotNull(blockMeta);
    long blockId = blockMeta.getBlockId();
    long blockSize = blockMeta.getBlockSize();

    if (getAvailableBytes() < blockSize) {
      StorageLevelAlias alias =
          StorageLevelAlias.getAlias(blockMeta.getBlockLocation().tierAlias());
      throw new OutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_META, blockId, blockSize,
          getAvailableBytes(), alias);
    }
    if (hasBlockMeta(blockId)) {
      StorageLevelAlias alias =
          StorageLevelAlias.getAlias(blockMeta.getBlockLocation().tierAlias());
      throw new AlreadyExistsException(ExceptionMessage.ADD_EXISTING_BLOCK, blockId, alias);
    }
    mBlockIdToBlockMap.put(blockId, blockMeta);
    reserveSpace(blockSize, true);
  }

  /**
   * Adds the metadata of a new block into this storage dir.
   *
   * @param tempBlockMeta the meta data of a temp block to add
   * @throws AlreadyExistsException if blockId already exists
   * @throws OutOfSpaceException when not enough space to hold block
   */
  public void addTempBlockMeta(TempBlockMeta tempBlockMeta)
      throws OutOfSpaceException, AlreadyExistsException {
    Preconditions.checkNotNull(tempBlockMeta);
    long userId = tempBlockMeta.getUserId();
    long blockId = tempBlockMeta.getBlockId();
    long blockSize = tempBlockMeta.getBlockSize();

    if (getAvailableBytes() < blockSize) {
      StorageLevelAlias alias =
          StorageLevelAlias.getAlias(tempBlockMeta.getBlockLocation().tierAlias());
      throw new OutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_META, blockId, blockSize,
          getAvailableBytes(), alias);
    }
    if (hasTempBlockMeta(blockId)) {
      StorageLevelAlias alias =
          StorageLevelAlias.getAlias(tempBlockMeta.getBlockLocation().tierAlias());
      throw new AlreadyExistsException(ExceptionMessage.ADD_EXISTING_BLOCK, blockId, alias);
    }

    mBlockIdToTempBlockMap.put(blockId, tempBlockMeta);
    Set<Long> userTempBlocks = mUserIdToTempBlockIdsMap.get(userId);
    if (userTempBlocks == null) {
      mUserIdToTempBlockIdsMap.put(userId, Sets.newHashSet(blockId));
    } else {
      userTempBlocks.add(blockId);
    }
    reserveSpace(blockSize, false);
  }

  /**
   * Removes a block from this storage dir.
   *
   * @param blockMeta the meta data of the block
   * @throws NotFoundException if no block is found
   */
  public void removeBlockMeta(BlockMeta blockMeta) throws NotFoundException {
    Preconditions.checkNotNull(blockMeta);
    long blockId = blockMeta.getBlockId();
    BlockMeta deletedBlockMeta = mBlockIdToBlockMap.remove(blockId);
    if (deletedBlockMeta == null) {
      throw new NotFoundException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
    }
    reclaimSpace(blockMeta.getBlockSize(), true);
  }

  /**
   * Removes a temp block from this storage dir.
   *
   * @param tempBlockMeta the meta data of the temp block to remove
   * @throws NotFoundException if no temp block is found
   */
  public void removeTempBlockMeta(TempBlockMeta tempBlockMeta) throws NotFoundException {
    Preconditions.checkNotNull(tempBlockMeta);
    final long blockId = tempBlockMeta.getBlockId();
    final long userId = tempBlockMeta.getUserId();
    TempBlockMeta deletedTempBlockMeta = mBlockIdToTempBlockMap.remove(blockId);
    if (deletedTempBlockMeta == null) {
      throw new NotFoundException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
    }
    Set<Long> userBlocks = mUserIdToTempBlockIdsMap.get(userId);
    if (userBlocks == null || !userBlocks.contains(blockId)) {
      StorageLevelAlias alias = StorageLevelAlias.getAlias(this.getDirIndex());
      throw new NotFoundException(ExceptionMessage.BLOCK_NOT_FOUND_FOR_USER, blockId, alias,
          userId);
    }
    Preconditions.checkState(userBlocks.remove(blockId));
    if (userBlocks.isEmpty()) {
      mUserIdToTempBlockIdsMap.remove(userId);
    }
    reclaimSpace(tempBlockMeta.getBlockSize(), false);
  }

  /**
   * Changes the size of a temp block.
   *
   * @param tempBlockMeta the meta data of the temp block to resize
   * @param newSize the new size after change in bytes
   * @throws InvalidStateException when newSize is smaller than oldSize
   */
  public void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize)
      throws InvalidStateException {
    long oldSize = tempBlockMeta.getBlockSize();
    if (newSize > oldSize) {
      reserveSpace(newSize - oldSize, false);
      tempBlockMeta.setBlockSize(newSize);
    } else if (newSize < oldSize) {
      throw new InvalidStateException("Shrinking block, not supported!");
    }
  }

  /**
   * Cleans up the temp block meta data for each block id passed in.
   *
   * @param userId the ID of the client associated with the temporary blocks
   * @param tempBlockIds the list of temporary blocks to clean up, non temporary blocks or
   *        nonexistent blocks will be ignored
   */
  public void cleanupUserTempBlocks(long userId, List<Long> tempBlockIds) {
    Set<Long> userTempBlocks = mUserIdToTempBlockIdsMap.get(userId);
    // The user's temporary blocks have already been removed.
    if (userTempBlocks == null) {
      return;
    }
    for (Long tempBlockId : tempBlockIds) {
      if (!mBlockIdToTempBlockMap.containsKey(tempBlockId)) {
        // This temp block does not exist in this dir, this is expected for some blocks since the
        // input list is across all dirs
        continue;
      }
      userTempBlocks.remove(tempBlockId);
      TempBlockMeta tempBlockMeta = mBlockIdToTempBlockMap.remove(tempBlockId);
      if (tempBlockMeta != null) {
        reclaimSpace(tempBlockMeta.getBlockSize(), false);
      } else {
        LOG.error("Cannot find blockId {} when cleanup userId {}", tempBlockId, userId);
      }
    }
    if (userTempBlocks.isEmpty()) {
      mUserIdToTempBlockIdsMap.remove(userId);
    } else {
      // This may happen if the client comes back during clean up and creates more blocks or some
      // temporary blocks failed to be deleted
      LOG.warn("Blocks still owned by user " + userId + " after cleanup.");
    }
  }

  /**
   * Gets the temporary blocks associated with a user in this StorageDir, an empty list is returned
   * if the user has no temporary blocks in this StorageDir.
   *
   * @param userId the ID of the user
   * @return A list of temporary blocks the user is associated with in this StorageDir
   */
  public List<TempBlockMeta> getUserTempBlocks(long userId) {
    Set<Long> userTempBlockIds = mUserIdToTempBlockIdsMap.get(userId);

    if (userTempBlockIds == null || userTempBlockIds.isEmpty()) {
      return Collections.emptyList();
    }
    List<TempBlockMeta> userTempBlocks = new ArrayList<TempBlockMeta>();
    for (long blockId : userTempBlockIds) {
      userTempBlocks.add(mBlockIdToTempBlockMap.get(blockId));
    }
    return userTempBlocks;
  }

  /**
   * @return the block store location of this directory.
   */
  public BlockStoreLocation toBlockStoreLocation() {
    return new BlockStoreLocation(mTier.getTierAlias(), mTier.getTierLevel(), mDirIndex);
  }

  private void reclaimSpace(long size, boolean committed) {
    Preconditions.checkState(mCapacityBytes >= mAvailableBytes.get() + size,
        "Available bytes should always be less than total capacity bytes");
    mAvailableBytes.addAndGet(size);
    if (committed) {
      mCommittedBytes.addAndGet(-size);
    }
  }

  private void reserveSpace(long size, boolean committed) {
    Preconditions.checkState(size <= mAvailableBytes.get(),
        "Available bytes should always be non-negative");
    mAvailableBytes.addAndGet(-size);
    if (committed) {
      mCommittedBytes.addAndGet(size);
    }
  }
}
