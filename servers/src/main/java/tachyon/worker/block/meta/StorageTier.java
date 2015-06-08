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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.conf.TachyonConf;
import tachyon.Users;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.allocator.AllocateStrategies;
import tachyon.worker.block.allocator.AllocateStrategy;
import tachyon.worker.block.allocator.AllocateStrategyType;
import tachyon.worker.block.evictor.EvictStrategies;
import tachyon.worker.block.evictor.EvictStrategy;
import tachyon.worker.block.evictor.EvictStrategyType;

/**
 * StorageTier manages StorageDirs, requests space for new coming blocks, and evicts stale blocks to
 * its successor StorageTier to get enough space requested. Each StorageTier contains several
 * StorageDirs. It is recommended to configure multiple StorageDirs in each StorageTier, to spread
 * out the I/O for better performance.
 */
public class StorageTier {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Storage level of current StorageTier */
  private final int mLevel;
  /** Alias of the current StorageTier's storage level */
  private final StorageLevelAlias mAlias;
  /** Successor StorageTier of the current StorageTier */
  private final StorageTier mNextTier;
  /** StorageDirs in the current StorageTier */
  private final StorageDir[] mDirs;
  /** Allocate space among StorageDirs by certain strategy */
  private final AllocateStrategy mSpaceAllocator;
  /** Evict block files to successor StorageTier by certain strategy */
  private final EvictStrategy mBlockEvictor;
  /** Capacity of the current StorageTier in bytes */
  private final long mCapacityBytes;
  /** The TachyonConf configuration properties */
  private final TachyonConf mTachyonConf;
  /** The WorkerSource instance in the metrics system */
  private final WorkerSource mWorkerSource;

  /**
   * Creates a new StorageTier
   *
   * @param level the level of the StorageTier
   * @param tachyonConf the TachyonConf configuration properties
   * @param nextTier the successor StorageTier
   * @param workerSource the WorkerSource instance in the metrics system
   * @throws IOException
   */
  public StorageTier(int level, TachyonConf tachyonConf, StorageTier nextTier,
      WorkerSource workerSource) throws IOException {
    mLevel = level;
    mTachyonConf = tachyonConf;
    String tierLevelAliasProp =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, level);
    mAlias = tachyonConf.getEnum(tierLevelAliasProp, StorageLevelAlias.MEM);
    String tierLevelDirPath =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, level);
    String[] dirPaths = tachyonConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
    for (int i = 0; i < dirPaths.length; i ++) {
      dirPaths[i] = dirPaths[i].trim();
    }
    String tierDirsQuotaProp =
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, level);
    // TODO: Figure out in which scenarios just using 'level' will not work.
    int indexQuota = Math.min(level, Constants.DEFAULT_STORAGE_TIER_DIR_QUOTA.length - 1);
    String[] tierDirsQuota =
        tachyonConf.get(tierDirsQuotaProp, Constants.DEFAULT_STORAGE_TIER_DIR_QUOTA[indexQuota])
        .split(",");
    // The storage directory quota for each storage directory
    long[] dirCapacities = new long[dirPaths.length];
    for (int i = 0; i < dirPaths.length; i ++) {
      // If not all dir quotas are specified, the last one will be used for remaining dirs.
      int index = Math.min(i, tierDirsQuota.length - 1);
      dirCapacities[i] = CommonUtils.parseSpaceSize(tierDirsQuota[index].trim());
    }
    String dataFolder =
        tachyonConf.get(Constants.WORKER_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);
    String userTempFolder =
        tachyonConf.get(Constants.WORKER_USER_TEMP_RELATIVE_FOLDER, "users");
    mDirs = new StorageDir[dirPaths.length];
    long quotaBytes = 0;
    for (int i = 0; i < dirPaths.length; i ++) {
      long storageDirId = StorageDirId.getStorageDirId(level, mAlias.getValue(), i);
      // TODO add conf for UFS
      mDirs[i] =
          new StorageDir(storageDirId, dirPaths[i], dirCapacities[i], dataFolder,
              CommonUtils.concatPath(dataFolder, userTempFolder), null, tachyonConf, workerSource);
      quotaBytes += dirCapacities[i];
    }
    mCapacityBytes = quotaBytes;
    mNextTier = nextTier;
    mWorkerSource = workerSource;
    mSpaceAllocator =
        AllocateStrategies.getAllocateStrategy(tachyonConf.getEnum(
            Constants.WORKER_ALLOCATE_STRATEGY_TYPE, AllocateStrategyType.MAX_FREE));
    mBlockEvictor =
        EvictStrategies.getEvictStrategy(
            tachyonConf.getEnum(Constants.WORKER_EVICT_STRATEGY_TYPE, EvictStrategyType.LRU),
            isLastTier());
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
   * Get the number of blocks in this StorageTier
   *
   * @return the number of blocks in this StorageTier
   */
  public int getNumberOfBlocks() {
    int ret = 0;
    for (StorageDir dir : mDirs) {
      ret += dir.getNumberOfBlocks();
    }
    return ret;
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
      dir.initialize();
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
      Pair<StorageDir, List<BlockInfo>> evictInfo =
          mBlockEvictor.getDirCandidate(dirs, pinList, requestSizeBytes);
      if (evictInfo == null) {
        // Nothing to evict. But some blocks may be deleted meanwhile, so retry to allocate
        // space again, if still failed, return null.
        return mSpaceAllocator.getStorageDir(dirs, userId, requestSizeBytes);
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
          mWorkerSource.incBlocksEvicted();
          LOG.debug("Evicted block Id:{}" + blockId);
        }
      }
      if (dirSelected.requestSpace(userId, requestSizeBytes)) {
        return dirSelected;
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
