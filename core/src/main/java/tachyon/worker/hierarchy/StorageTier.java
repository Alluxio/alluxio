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

package tachyon.worker.hierarchy;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.Users;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;
import tachyon.worker.allocation.AllocateStrategies;
import tachyon.worker.allocation.AllocateStrategy;
import tachyon.worker.eviction.EvictStrategies;
import tachyon.worker.eviction.EvictStrategy;

/**
 * StorageTier manages StorageDirs, requests space for new coming blocks, and evicts old blocks to
 * its successor StorageTier to get enough space requested. Each StorageTier contains several
 * StorageDirs. It is recommended to configure multiple StorageDirs in each StorageTier, to spread
 * out the I/O for better performance.
 */
public class StorageTier {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Storage level of current StorageTier */
  private final int mLevel;
  /** Alias of current StorageTier's storage level */
  private final StorageLevelAlias mAlias;
  /** Successor StorageTier of current StorageTier */
  private final StorageTier mNextTier;
  /** StorageDirs in current StorageTier */
  private final StorageDir[] mDirs;
  /** Allocate space among StorageDirs by certain strategy */
  private final AllocateStrategy mSpaceAllocator;
  /** Evict block files to successor StorageTier by certain strategy */
  private final EvictStrategy mBlockEvictor;
  /** Capacity of current StorageTier in bytes */
  private final long mCapacityBytes;
  /** Max retry times when requesting space from current StorageTier */
  private static final int FAILED_SPACE_REQUEST_LIMITS = UserConf.get().FAILED_SPACE_REQUEST_LIMITS;

  /**
   * Creates a new StorageTier
   * 
   * @param storageLevel the level of the StorageTier
   * @param storageLevelAlias the alias of the StorageTier's storage level
   * @param dirPaths paths of StorageDirs in the StorageTier
   * @param dirCapacityBytes capacities of StorageDirs in the StorageTier
   * @param dataFolder data folder in the StorageDir
   * @param userTempFolder user temporary folder in the StorageDir
   * @param nextTier the successor StorageTier
   * @param conf configuration of StorageDir
   * @throws IOException
   */
  public StorageTier(int storageLevel, StorageLevelAlias storageLevelAlias, String[] dirPaths,
      long[] dirCapacityBytes, String dataFolder, String userTempFolder, StorageTier nextTier,
      Object conf) throws IOException {
    mLevel = storageLevel;
    mAlias = storageLevelAlias;
    mDirs = new StorageDir[dirPaths.length];
    long quotaBytes = 0;
    for (int i = 0; i < dirPaths.length; i++) {
      long storageDirId = StorageDirId.getStorageDirId(storageLevel, mAlias.getValue(), i);
      mDirs[i] =
          new StorageDir(storageDirId, dirPaths[i], dirCapacityBytes[i], dataFolder,
              userTempFolder, conf);
      quotaBytes += dirCapacityBytes[i];
    }
    mCapacityBytes = quotaBytes;
    mNextTier = nextTier;
    mSpaceAllocator =
        AllocateStrategies.getAllocateStrategy(WorkerConf.get().ALLOCATE_STRATEGY_TYPE);
    mBlockEvictor =
        EvictStrategies.getEvictStrategy(WorkerConf.get().EVICT_STRATEGY_TYPE, isLastTier());
  }

  /**
   * Check whether certain block exists in current StorageTier
   * 
   * @param blockId id of the block
   * @return true if the block exists in current StorageTier, false otherwise
   */
  public boolean containsBlock(long blockId) {
    return getStorageDirByBlockId(blockId) != null;
  }

  /**
   * Get capacity of current StorageTier in bytes
   * 
   * @return capacity of StorageTier in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * Get next StorageTier
   * 
   * @return next StorageTier
   */
  public StorageTier getNextStorageTier() {
    return mNextTier;
  }

  /**
   * Find the StorageDir which contains the given block Id
   * 
   * @param blockId the id of the block
   * @return StorageDir which contains the block, null if none of StorageDir contains the block.
   */
  public StorageDir getStorageDirByBlockId(long blockId) {
    for (StorageDir dir : mDirs) {
      if (dir.containsBlock(blockId)) {
        return dir;
      }
    }
    return null;
  }

  /**
   * Get StorageDir by array index
   * 
   * @param dirIndex index of the StorageDir
   * @return StorageDir selected, null if index out of boundary
   */
  public StorageDir getStorageDirByIndex(int dirIndex) {
    if (dirIndex < mDirs.length && dirIndex >= 0) {
      return mDirs[dirIndex];
    }
    return null;
  }

  /**
   * Get StorageDirs in current StorageTier
   * 
   * @return StorageDirs in current StorageTier
   */
  public StorageDir[] getStorageDirs() {
    // TODO This method should be removed to prevent exposing StorageDirs
    return mDirs;
  }

  /**
   * Get the storage level of the StorageTier
   * 
   * @return the storage level of the StorageTier
   */
  public int getLevel() {
    return mLevel;
  }

  /**
   * Get the alias of the StorageTier's storage level
   * 
   * @return the alias of the StorageTier's storage level
   */
  public StorageLevelAlias getAlias() {
    return mAlias;
  }

  /**
   * Get used space in the StorageTier
   * 
   * @return used space size in bytes
   */
  public long getUsedBytes() {
    long used = 0;
    for (StorageDir dir : mDirs) {
      used += dir.getUsedBytes();
    }
    return used;
  }

  /**
   * Initialize StorageDirs in current StorageTier
   * 
   * @throws IOException
   */
  public void initialize() throws IOException {
    for (StorageDir dir : mDirs) {
      dir.initailize();
    }
  }

  /**
   * Check whether the StorageTier is the last tier
   * 
   * @return true if the StorageTier is the last tier, false otherwise
   */
  public boolean isLastTier() {
    return mNextTier == null;
  }

  /**
   * Request space from any StorageDir in the StorageTier.
   * 
   * @param userId the id of the user
   * @param requestBytes requested space in bytes
   * @param pinList list of pinned files
   * @param removedBlockIds list of blocks which are removed from Tachyon
   * @return the StorageDir assigned.
   * @throws IOException
   */
  public StorageDir requestSpace(long userId, long requestBytes, Set<Integer> pinList,
      List<Long> removedBlockIds) throws IOException {
    return requestSpace(mDirs, userId, requestBytes, pinList, removedBlockIds);
  }

  /**
   * Request space from specified StorageDir in the StorageTier.
   * 
   * @param storageDir StorageDir that the space will be allocated in
   * @param userId id of the user
   * @param requestBytes size to request in bytes
   * @param pinList list of pinned files
   * @param removedBlockIds list of blocks which are removed from Tachyon
   * @return true if allocate successfully, false otherwise.
   * @throws IOException
   */
  public boolean requestSpace(StorageDir storageDir, long userId, long requestBytes,
      Set<Integer> pinList, List<Long> removedBlockIds) throws IOException {
    if (StorageDirId.getStorageLevel(storageDir.getStorageDirId()) != mLevel) {
      return false;
    }
    StorageDir[] dirs = new StorageDir[1];
    dirs[0] = storageDir;
    return storageDir == requestSpace(dirs, userId, requestBytes, pinList, removedBlockIds);
  }

  /**
   * Request space from StorageDir candidates in the StorageTier.
   * 
   * @param dirs candidates of StorageDirs to allocate space
   * @param userId id of the user
   * @param requestSizeBytes size to request in bytes
   * @param pinList list of pinned files
   * @param removedBlockIds list of blocks which are removed from Tachyon
   * @return the StorageDir assigned.
   * @throws IOException
   */
  // TODO make block eviction asynchronous, then no need to be synchronized
  private synchronized StorageDir requestSpace(StorageDir[] dirs, long userId,
      long requestSizeBytes, Set<Integer> pinList, List<Long> removedBlockIds) throws IOException {
    StorageDir dirSelected = mSpaceAllocator.getStorageDir(dirs, userId, requestSizeBytes);
    if (dirSelected != null) {
      return dirSelected;
    }

    if (mSpaceAllocator.fitInPossible(dirs, requestSizeBytes)) {
      for (int attempt = 0; attempt < FAILED_SPACE_REQUEST_LIMITS; attempt ++) {
        Pair<StorageDir, List<BlockInfo>> evictInfo =
            mBlockEvictor.getDirCandidate(dirs, pinList, requestSizeBytes);
        if (evictInfo == null) {
          return null;
        }
        dirSelected = evictInfo.getFirst();
        List<BlockInfo> blocksInfoList = evictInfo.getSecond();
        for (BlockInfo blockInfo : blocksInfoList) {
          StorageDir dir = blockInfo.getStorageDir();
          if (!dir.isBlockLocked(blockInfo.getBlockId())) { // pinList is not updated
            long blockId = blockInfo.getBlockId();
            if (isLastTier()) {
              dir.deleteBlock(blockId);
              removedBlockIds.add(blockId);
            } else {
              StorageDir dstDir =
                  mNextTier.requestSpace(Users.MIGRATE_DATA_USER_ID, blockInfo.getSize(), pinList,
                      removedBlockIds);
              dir.moveBlock(blockId, dstDir);
            }
            LOG.debug("Evicted block Id:{}" + blockId);
          }
        }
        if (dirSelected.requestSpace(userId, requestSizeBytes)) {
          return dirSelected;
        } else {
          LOG.warn("Request space failed! attempt:{} storageLevel:{}", attempt, mLevel);
        }
      }
    }
    LOG.warn("No StorageDir is allocated! requestSize:{} storageLevel:{} used:{} capacity:{}",
        requestSizeBytes, mLevel, getUsedBytes(), getCapacityBytes());
    return null;
  }

  @Override
  public String toString() {
    return mLevel + "_" + mAlias;
  }
}
