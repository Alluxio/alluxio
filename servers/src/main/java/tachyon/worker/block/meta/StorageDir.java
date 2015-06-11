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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;

/**
 * Represents a directory in a storage tier. It has a fixed capacity allocated to it on
 * instantiation. It contains the set of blocks currently in the storage directory
 * <p>
 * This class does not guarantee thread safety.
 */
public class StorageDir {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private Map<Long, BlockMeta> mBlockIdToBlockMap;
  private Map<Long, Set<Long>> mUserIdToBlockIdsMap;
  private Map<Long, TempBlockMeta> mBlockIdToTempBlockMap;
  private Map<Long, Set<Long>> mUserIdToTempBlockIdsMap;

  private final long mCapacityBytes;
  private long mAvailableBytes;
  private String mDirPath;
  private int mDirId;
  private StorageTier mTier;

  public StorageDir(StorageTier tier, int dirId, long capacityBytes, String dirPath) {
    mTier = Preconditions.checkNotNull(tier);
    mDirId = dirId;
    mCapacityBytes = capacityBytes;
    mAvailableBytes = capacityBytes;
    mDirPath = dirPath;
    mBlockIdToBlockMap = new HashMap<Long, BlockMeta>(200);
    mUserIdToBlockIdsMap = new HashMap<Long, Set<Long>>(20);
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

  public StorageTier getParentTier() {
    return mTier;
  }

  public int getDirId() {
    return mDirId;
  }

  // TODO: deprecate this method.
  public long getStorageDirId() {
    // TODO: implement me
    return 0;
  }

  /**
   * Returns a list of (non-temp) block IDs in this dir.
   */
  public List<Long> getBlockIds() {
    List<Long> blockIds = new ArrayList<Long>();
    for (long blockId: mBlockIdToBlockMap.keySet()) {
      blockIds.add(blockId);
    }
    return blockIds;
  }

  /**
   * Check if a specific block is in this storage dir.
   *
   * @param blockId the block ID
   * @return true if the block is in this storage dir, false otherwise
   */
  public boolean hasBlockMeta(long blockId) {
    return mBlockIdToBlockMap.containsKey(blockId)
  }

  /**
   * Check if a temp block is in this storage dir.
   *
   * @param blockId the block ID
   * @return true if the block is in this storage dir, false otherwise
   */
  public boolean hasTempBlockMeta(long blockId) {
    return mBlockIdToTempBlockMap.containsKey(blockId)
  }

  /**
   * Get the BlockMeta from this storage dir by its block ID.
   *
   * @param blockId the block ID
   * @return the BlockMeta or absent
   */
  public Optional<BlockMeta> getBlockMeta(long blockId) {
    return Optional.fromNullable(mBlockIdToBlockMap.get(blockId));
  }

  /**
   * Get the BlockMeta from this storage dir by its block ID.
   *
   * @param blockId the block ID
   * @return the BlockMeta or absent
   */
  public Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    return Optional.fromNullable(mBlockIdToTempBlockMap.get(blockId));
  }

  /**
   * Add the metadata of a new block into this storage dir.
   *
   * @param userId the user ID
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @return the BlockMeta or absent
   */
  public Optional<BlockMeta> addBlockMeta(long userId, long blockId, long blockSize) {
    if (getAvailableBytes() < blockSize) {
      LOG.error("Fail to create blockId {} in dir {}: {} bytes required, but {} bytes available",
          blockId, toString(), blockSize, getAvailableBytes());
      return Optional.absent();
    }
    if (hasBlockMeta(blockId)) {
      LOG.error("Fail to create blockId {} in dir {}: blockId exists", blockId, toString());
      return Optional.absent();
    }
    Set<Long> userBlocks = mUserIdToBlockIdsMap.get(userId);
    if (null == userBlocks) {
      mUserIdToBlockIdsMap.put(userId, Sets.newHashSet(blockId));
    } else {
      userBlocks.add(blockId);
    }
    BlockMeta block = new BlockMeta(blockId, blockSize, this);
    mBlockIdToBlockMap.put(userId, block);
    mAvailableBytes -= blockSize;
    Preconditions.checkState(mAvailableBytes >= 0, "Available bytes should always be non-negative");
    return Optional.of(block);
  }

  /**
   * Add the metadata of a new block into this storage dir.
   *
   * @param tempBlockMeta
   * @return the BlockMeta or absent
   */
  public boolean addTempBlockMeta(TempBlockMeta tempBlockMeta) {
    long userId = tempBlockMeta.getUserId();
    long blockId = tempBlockMeta.getBlockId();
    long blockSize = tempBlockMeta.getBlockSize();
    mBlockIdToTempBlockMap.put(blockId, tempBlockMeta);
    Set<Long> userTempBlocks = mUserIdToTempBlockIdsMap.get(userId);
    if (null == userTempBlocks) {
      mUserIdToBlockIdsMap.put(userId, Sets.newHashSet(blockId));
    } else {
      userTempBlocks.add(blockId);
    }
    mCapacityBytes += blockSize;
    mAvailableBytes -= blockSize;
    return true;
  }

  /**
   * Remove a block from this storage dir.
   *
   * @param blockId the block ID
   * @return true if success, false otherwise
   */
  public boolean removeBlockMeta(long blockId) {
    if (!hasBlockMeta(blockId)) {
      return false;
    }
    BlockMeta block = mBlockIdToBlockMap.remove(blockId);
    Preconditions.checkNotNull(block);
    for (Map.Entry<Long, Set<Long>> entry : mUserIdToBlockIdsMap.entrySet()) {
      Long userId = entry.getKey();
      Set<Long> userBlocks = entry.getValue();
      if (userBlocks.contains(blockId)) {
        Preconditions.checkState(userBlocks.remove(blockId));
        if (userBlocks.isEmpty()) {
          mUserIdToBlockIdsMap.remove(userId);
        }
        mCapacityBytes -= block.getBlockSize();
        mAvailableBytes += block.getBlockSize();
        Preconditions.checkState(mCapacityBytes >= 0, "Capacity bytes should always be "
            + "non-negative");
        return true;
      }
    }
    return false;
  }

  /**
   * Remove a block from this storage dir.
   *
   * @param blockId the block ID
   * @return true if success, false otherwise
   */
  public boolean removeTempBlockMeta(long blockId) {
    if (!hasTempBlockMeta(blockId)) {
      return false;
    }
    TempBlockMeta tempBlockMeta = mBlockIdToTempBlockMap.remove(blockId);
    Preconditions.checkNotNull(tempBlockMeta);
    for (Map.Entry<Long, Set<Long>> entry : mUserIdToBlockIdsMap.entrySet()) {
      Long userId = entry.getKey();
      Set<Long> userBlocks = entry.getValue();
      if (userBlocks.contains(blockId)) {
        Preconditions.checkState(userBlocks.remove(blockId));
        if (userBlocks.isEmpty()) {
          mUserIdToBlockIdsMap.remove(userId);
        }
        mAvailableBytes += tempBlockMeta.getBlockSize();
        Preconditions.checkState(mCapacityBytes >= 0, "Capacity bytes should always be "
            + "non-negative");
        return true;
      }
    }
    return false;
  }
}
