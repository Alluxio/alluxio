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
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.StorageDirId;

/**
 * Represents a directory in a storage tier. It has a fixed capacity allocated to it on
 * instantiation. It contains the set of blocks currently in the storage directory
 * <p>
 * This class does not guarantee thread safety.
 */
public class StorageDir {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final long mCapacityBytes;
  /** A map from block ID to block meta data */
  private Map<Long, BlockMeta> mBlockIdToBlockMap;
  /** A map from block ID to temp block meta data */
  private Map<Long, TempBlockMeta> mBlockIdToTempBlockMap;
  /** A map from user ID to the set of temp blocks created by this user */
  private Map<Long, Set<Long>> mUserIdToTempBlockIdsMap;
  private long mAvailableBytes;
  private String mDirPath;
  private int mDirIndex;
  private StorageTier mTier;

  public StorageDir(StorageTier tier, int dirIndex, long capacityBytes, String dirPath) {
    mTier = Preconditions.checkNotNull(tier);
    mDirIndex = dirIndex;
    mCapacityBytes = capacityBytes;
    mAvailableBytes = capacityBytes;
    mDirPath = dirPath;
    mBlockIdToBlockMap = new HashMap<Long, BlockMeta>(200);
    mBlockIdToTempBlockMap = new HashMap<Long, TempBlockMeta>(200);
    mUserIdToTempBlockIdsMap = new HashMap<Long, Set<Long>>(200);

    initializeMeta();
  }

  /**
   * Only the File that is file and filename can be parsed into Long will be accept
   */
  class MetaFileFilter implements FileFilter {
    public boolean accept(File path) {
      if (!path.isFile()) {
        return false;
      }
      try {
        Long.valueOf(path.getName());
        return true;
      } catch (NumberFormatException nfe) {
        return false;
      }
    }
  }

  /**
   * Initialize meta data for existing blocks in this StorageDir
   *
   * Only paths satisfying the contract defined in {@link BlockMetaBase#commitPath()} are legal,
   * should be in format like {dir}/{blockId}. others are ignored.
   */
  private void initializeMeta() {
    File dir = new File(mDirPath);
    FileFilter filter = new MetaFileFilter();
    File[] files = dir.listFiles(filter);
    if (files == null) {
      return;
    }
    for (File file : files) {
      try {
        long blockId = Long.valueOf(file.getName());
        addBlockMeta(new BlockMeta(blockId, file.length(), this));
      } catch (IOException ioe) {
        LOG.warn("can not add block meta of file %s: %s", file.getAbsolutePath(), ioe);
      }
    }
  }

  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  public long getAvailableBytes() {
    return mAvailableBytes;
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
   * Gets the BlockMeta from this storage dir by its block ID or throws IOException.
   *
   * @param blockId the block ID
   * @return BlockMeta of the given block or null
   * @throws IOException if no block is found
   */
  public BlockMeta getBlockMeta(long blockId) throws IOException {
    BlockMeta blockMeta = mBlockIdToBlockMap.get(blockId);
    if (blockMeta == null) {
      throw new IOException("Failed to get BlockMeta: blockId " + blockId + " not found in "
          + toString());
    }
    return blockMeta;
  }

  /**
   * Gets the BlockMeta from this storage dir by its block ID or throws IOException.
   *
   * @param blockId the block ID
   * @return TempBlockMeta of the given block or null
   * @throws IOException if no temp block is found
   */
  public TempBlockMeta getTempBlockMeta(long blockId) throws IOException {
    TempBlockMeta tempBlockMeta = mBlockIdToTempBlockMap.get(blockId);
    if (tempBlockMeta == null) {
      throw new IOException("Failed to get TempBlockMeta: blockId " + blockId + " not found in "
          + toString());
    }
    return tempBlockMeta;
  }

  /**
   * Adds the metadata of a new block into this storage dir or throws IOException.
   *
   * @param blockMeta the meta data of the block
   * @throws IOException if blockId already exists or not enough space
   */
  public void addBlockMeta(BlockMeta blockMeta) throws IOException {
    Preconditions.checkNotNull(blockMeta);
    long blockId = blockMeta.getBlockId();
    long blockSize = blockMeta.getBlockSize();

    if (getAvailableBytes() < blockSize) {
      throw new IOException("Failed to add BlockMeta: blockId " + blockId + " is " + blockSize
          + " bytes, but only " + getAvailableBytes() + " bytes available");
    }
    if (hasBlockMeta(blockId)) {
      throw new IOException("Failed to add BlockMeta: blockId " + blockId + " exists");
    }
    mBlockIdToBlockMap.put(blockId, blockMeta);
    reserveSpace(blockSize);
  }

  /**
   * Adds the metadata of a new block into this storage dir or throws IOException.
   *
   * @param tempBlockMeta the meta data of a temp block to add
   * @throws IOException if blockId already exists or not enough space
   */
  public void addTempBlockMeta(TempBlockMeta tempBlockMeta) throws IOException {
    Preconditions.checkNotNull(tempBlockMeta);
    long userId = tempBlockMeta.getUserId();
    long blockId = tempBlockMeta.getBlockId();
    long blockSize = tempBlockMeta.getBlockSize();

    if (getAvailableBytes() < blockSize) {
      throw new IOException("Failed to add TempBlockMeta: blockId " + blockId + " is " + blockSize
          + " bytes, but only " + getAvailableBytes() + " bytes available");
    }
    if (hasTempBlockMeta(blockId)) {
      throw new IOException("Failed to add TempBlockMeta: blockId " + blockId + " exists");
    }

    mBlockIdToTempBlockMap.put(blockId, tempBlockMeta);
    Set<Long> userTempBlocks = mUserIdToTempBlockIdsMap.get(userId);
    if (userTempBlocks == null) {
      mUserIdToTempBlockIdsMap.put(userId, Sets.newHashSet(blockId));
    } else {
      userTempBlocks.add(blockId);
    }
    reserveSpace(blockSize);
  }

  /**
   * Removes a block from this storage dir or throws IOException.
   *
   * @param blockMeta the meta data of the block
   * @throws IOException if no block is found
   */
  public void removeBlockMeta(BlockMeta blockMeta) throws IOException {
    Preconditions.checkNotNull(blockMeta);
    long blockId = blockMeta.getBlockId();
    BlockMeta deletedBlockMeta = mBlockIdToBlockMap.remove(blockId);
    if (deletedBlockMeta == null) {
      throw new IOException("Failed to remove BlockMeta: blockId " + blockId + " not found");
    }
    reclaimSpace(blockMeta.getBlockSize());
  }

  /**
   * Removes a temp block from this storage dir or throws IOException.
   *
   * @param tempBlockMeta the meta data of the temp block to remove
   * @throws IOException if no temp block is found
   */
  public void removeTempBlockMeta(TempBlockMeta tempBlockMeta) throws IOException {
    Preconditions.checkNotNull(tempBlockMeta);
    final long blockId = tempBlockMeta.getBlockId();
    final long userId = tempBlockMeta.getUserId();
    TempBlockMeta deletedTempBlockMeta = mBlockIdToTempBlockMap.remove(blockId);
    if (deletedTempBlockMeta == null) {
      throw new IOException("Failed to remove TempBlockMeta: blockId " + blockId + " not found");
    }
    Set<Long> userBlocks = mUserIdToTempBlockIdsMap.get(userId);
    if (userBlocks == null) {
      throw new IOException("Failed to remove TempBlockMeta: blockId " + blockId + " has userId "
          + userId + " not found");
    }
    if (!userBlocks.contains(blockId)) {
      throw new IOException("Failed to remove TempBlockMeta: blockId " + blockId + " not "
          + "associated with userId " + userId);
    }
    Preconditions.checkState(userBlocks.remove(blockId));
    if (userBlocks.isEmpty()) {
      mUserIdToTempBlockIdsMap.remove(userId);
    }
    reclaimSpace(tempBlockMeta.getBlockSize());
  }

  /**
   * Changes the size of a temp block or throws IOException.
   *
   * @param tempBlockMeta the meta data of the temp block to resize
   * @param newSize the new size after change in bytes
   * @throws IOException
   */
  public void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize) throws IOException {
    long oldSize = tempBlockMeta.getBlockSize();
    tempBlockMeta.setBlockSize(newSize);
    if (newSize > oldSize) {
      reserveSpace(newSize - oldSize);
    } else if (newSize < oldSize) {
      throw new IOException("Shrinking block, not supported!");
    }
  }

  private void reserveSpace(long size) {
    Preconditions.checkState(size <= mAvailableBytes,
        "Available bytes should always be non-negative ");
    mAvailableBytes -= size;
  }

  private void reclaimSpace(long size) {
    Preconditions.checkState(mCapacityBytes >= mAvailableBytes + size,
        "Available bytes should always be less than total capacity bytes");
    mAvailableBytes += size;
  }

  /**
   * Cleans up the temp block meta data of a specific user.
   *
   * @param userId the ID of the user to cleanup
   * @return A list of temp blocks removed from this dir
   */
  public List<TempBlockMeta> cleanupUser(long userId) {
    List<TempBlockMeta> blocksToRemove = new ArrayList<TempBlockMeta>();
    Set<Long> userTempBlocks = mUserIdToTempBlockIdsMap.get(userId);
    if (userTempBlocks != null) {
      for (long blockId : userTempBlocks) {
        TempBlockMeta tempBlock = mBlockIdToTempBlockMap.remove(blockId);
        if (tempBlock != null) {
          blocksToRemove.add(tempBlock);
        } else {
          LOG.error("Cannot find blockId {} when cleanup userId {}", blockId, userId);
        }
      }
      mUserIdToTempBlockIdsMap.remove(userId);
    }
    return blocksToRemove;
  }
}
