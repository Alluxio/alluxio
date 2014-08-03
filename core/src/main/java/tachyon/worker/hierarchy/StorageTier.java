/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.hierarchy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.StorageId;
import tachyon.StorageLevelAlias;
import tachyon.worker.elimination.EvictLRU;
import tachyon.worker.elimination.EvictStrategy;
import tachyon.worker.elimination.BlockEvictionInfo;

/**
 * Hierarchy Storage Tier management
 * 
 */
public class StorageTier {
  /**
   * The user request certain space from the current storage tier.
   * It will return back the affordable StorageDir according to strategy(either random or
   * round-robbin). Otherwise it returns null if no more space available on mStorageDirs.
   */
  abstract class SpaceAllocator {
    public boolean evictPossible(long requestSize) {
      boolean isPossible = false;
      for (int i = 0; i < mStorageDirs.length; i ++) {
        if (mStorageDirs[i].getCapacity() >= requestSize) {
          isPossible = true;
          break;
        }
      }
      return isPossible;
    }

    /**
     * allocate requested size space in current storage dirs
     * 
     * @param requestSize
     * @return index of assigned storage dir, -1 if failed
     */
    abstract public int getAvailableDirIndex(long requestSize);
  }

  /**
   * allocate space randomly
   */
  public class SpaceAllocatorRandom extends SpaceAllocator {
    Random rand = new Random(System.currentTimeMillis());

    @Override
    public int getAvailableDirIndex(long size) {
      int availableDirIndex = -1;
      int i = rand.nextInt(mStorageDirs.length);
      for (int j = 0; j < mStorageDirs.length; i ++, j ++) {
        if (i == mStorageDirs.length) {
          i = 0;
        }
        if (mStorageDirs[i].getAvailable() >= size) {
          availableDirIndex = i;
          break;
        }
      }
      return availableDirIndex;
    }
  }

  /**
   * allocate space by round robin
   */
  public class SpaceAllocatorRR extends SpaceAllocator {
    int mDirIndex = 0;

    @Override
    public int getAvailableDirIndex(long size) {
      int availableDirIndex = -1;
      for (int j = 0; j < mStorageDirs.length; mDirIndex ++, j ++) {
        if (mDirIndex == mStorageDirs.length) {
          mDirIndex = 0;
        }
        if (mStorageDirs[mDirIndex].getAvailable() >= size) {
          availableDirIndex = mDirIndex;
          mDirIndex ++;
          break;
        }
      }
      return availableDirIndex;
    }
  }

  // The storage level for current storage tier
  protected final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final int mStorageLevel;
  private final StorageLevelAlias mAlias;
  private StorageTier mNextStorageTier = null;

  private final StorageDir[] mStorageDirs;
  private final SpaceAllocator mSpaceAllocator;
  private final EvictStrategy mBlockEvictor;
  private long mCapacity;
  private static final int mRequestSpaceMaxTryTime = 10;

  public StorageTier(int level, String alias, String[] dirPaths, long[] dirCapacities,
      String dataFolder, String userFolder, Object conf) throws IOException {
    mStorageLevel = level;
    int storageDirNum = dirPaths.length;
    mAlias = StorageLevelAlias.getStorageLevel(alias);
    mStorageDirs = new StorageDir[storageDirNum];
    for (int i = 0; i < storageDirNum; i ++) {
      // The storage directory quota for each storage directory
      long storageId = StorageId.getStorageId(level, mAlias.getValue(), i);
      mStorageDirs[i] =
          new StorageDir(storageId, dirPaths[i], dirCapacities[i], dataFolder, userFolder, conf);
      mCapacity += dirCapacities[i];
    }
    // TODO can be configured
    mSpaceAllocator = new SpaceAllocatorRandom();
    mBlockEvictor = new EvictLRU(mStorageDirs);
  }

  public long getCapacity() {
    return mCapacity;
  }

  /**
   * get next storage tier
   * 
   * @return next storage tier
   */
  public StorageTier getNextStorageTier() {
    return mNextStorageTier;
  }

  public List<Long> getRemovedBlockList() {
    List<Long> removedBlocks = new ArrayList<Long>();
    for (StorageDir dir : mStorageDirs) {
      removedBlocks.addAll(dir.getRemovedBlockList());
    }
    return removedBlocks;
  }

  /**
   * Find the storageDir for certain blockId
   * 
   * @param blockId
   *          the id of the block
   * @return storage dir that contain the block
   */
  public StorageDir getStorageDirByBlockId(long blockId) {
    StorageDir foundDir = null;
    for (StorageDir dir : mStorageDirs) {
      if (dir.containsBlock(blockId)) {
        foundDir = dir;
        break;
      }
    }
    return foundDir;
  };

  /**
   * get storage dir by index
   * 
   * @param dirIndex
   *          index of the storage dir
   * @return the chosen storage dir, null if index out of boundary
   */
  public StorageDir getStorageDirByIndex(int dirIndex) {
    if (dirIndex < mStorageDirs.length && dirIndex >= 0) {
      return mStorageDirs[dirIndex];
    } else {
      return null;
    }
  }

  /**
   * get storage dirs in current storage tier
   * 
   * @return array of storage dirs in current tier
   */
  public StorageDir[] getStorageDirs() {
    return mStorageDirs;
  }

  /**
   * get storageLevel of current storage tier
   * 
   * @return storage level
   */
  public int getStorageLevel() {
    return mStorageLevel;
  }

  public StorageLevelAlias getStorageLevelAlias() {
    return mAlias;
  }

  /**
   * get used space in current storage tier
   * 
   * @return used space size
   */
  public long getUsed() {
    long used = 0;
    for (StorageDir dir : mStorageDirs) {
      used += dir.getUsed();
    }
    return used;
  }

  /**
   * initialize storage dirs in current storage tier
   * 
   * @throws IOException
   */
  public void initialize() throws IOException {
    for (StorageDir dir : mStorageDirs) {
      dir.initailize();
    }
  }

  /**
   * check whether current tier is the last tier
   * 
   * @return true if current tier is the last tier, false otherwise
   */
  public boolean isLastTier() {
    return mNextStorageTier == null;
  }

  /**
   * request certain space from current tier
   * 
   * @param userId
   *          id of the user
   * @param requestSize
   *          size to request
   * @param pinList
   *          pinned files
   * @return the storage dir assigned.
   * @throws IOException
   */
  public StorageDir requestSpace(long userId, long requestSize, Set<Integer> pinList)
      throws IOException {
    // TODO make it asynchronous
    int dirIndex = mSpaceAllocator.getAvailableDirIndex(requestSize);
    if (dirIndex != -1) {
      mStorageDirs[dirIndex].requestSpace(userId, requestSize);
      return mStorageDirs[dirIndex];
    } else if (mSpaceAllocator.evictPossible(requestSize)) {
      for (int attempt = 0; attempt < mRequestSpaceMaxTryTime; attempt ++) {
        boolean lastTier = isLastTier();
        List<BlockEvictionInfo> blocksToEvict = new ArrayList<BlockEvictionInfo>();
        dirIndex = mBlockEvictor.getDirCandidate(blocksToEvict, pinList, lastTier, requestSize);
        for (BlockEvictionInfo blockInfo : blocksToEvict) {
          StorageDir srcDir = mStorageDirs[blockInfo.getDirIndex()];
          synchronized (srcDir.getLastBlockAccessTime()) {
            Map<Long, Set<Long>> lockedBlocks = srcDir.getUsersPerLockedBlock();
            synchronized (lockedBlocks) {
              if (!lockedBlocks.containsKey(blockInfo.getBlockId())) {
                if (lastTier) {
                  srcDir.deleteBlock(blockInfo.getBlockId());
                } else {
                  StorageDir dstDir =
                      mNextStorageTier.requestSpace(userId, blockInfo.getBlockSize(), pinList);
                  srcDir.moveBlock(blockInfo.getBlockId(), dstDir);
                }
              }
            }
          }
        }
        if (mStorageDirs[dirIndex].requestSpace(userId, requestSize)) {
          return mStorageDirs[dirIndex];
        } else {
          LOG.warn("request space attempt failed! attempt time:" + attempt + " storage level:"
              + mStorageLevel + " dir index:" + dirIndex);
        }
      }
    }
    throw new IOException("no dir is allocated!");
  }

  /**
   * @param storageTier
   *          the next storage tier
   */
  public void setNextStorageTier(StorageTier storageTier) {
    mNextStorageTier = storageTier;
  }

  @Override
  public String toString() {
    return mStorageLevel + "_" + mAlias;
  }
}
