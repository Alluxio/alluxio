package tachyon.worker.hierarchy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;
import tachyon.worker.eviction.EvictStrategies;
import tachyon.worker.eviction.EvictStrategy;

/**
 * Used to manage StorageDirs, request space for new coming blocks, and evict old blocks to its
 * successor StorageTier to get enough space for new coming blocks. Each StorageTier contains
 * several StorageDirs, which is configurable. It recommends to configure multiple StorageDirs in
 * each storage layer, to spread out the I/O while doing the eviction for better performance.
 */
public class StorageTier {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Storage level of current StorageTier */
  private final int mStorageLevel;
  /** Alias of current StorageTier's storage level */
  private final StorageLevelAlias mStorageLevelAlias;
  /** Successor StorageTier of current StorageTier */
  private final StorageTier mNextStorageTier;
  /** StorageDirs in current StorageTier */
  private final StorageDir[] mStorageDirs;
  /** Allocate space among StorageDirs by certain strategy */
  private final AllocateStrategy mSpaceAllocator;
  /** Evict block files to successor StorageTier by certain strategy */
  private final EvictStrategy mBlockEvictor;
  /** Capacity of current StorageTier in bytes */
  private final long mCapacityBytes;
  /** Max retry times when requesting space from current StorageTier */
  private static final int mRequestSpaceMaxTryTimes = UserConf.get().FAILED_SPACE_REQUEST_LIMITS;

  /**
   * Create a new StorageTier
   * 
   * @param level storage level of StorageTier
   * @param alias storage level alias of StorageTier
   * @param dirPaths
   * @param dirCapacitieBytes
   * @param dataFolder
   * @param userFolder
   * @param nextTier
   * @param conf
   * @throws IOException
   */
  public StorageTier(int storageLevel, String storageLevelAlias, String[] dirPaths,
      long[] dirCapacitieBytes, String dataFolder, String userFolder, StorageTier nextTier,
      Object conf) throws IOException {
    mStorageLevel = storageLevel;
    int storageDirNum = dirPaths.length;
    mStorageLevelAlias = StorageLevelAlias.getStorageLevel(storageLevelAlias);
    mStorageDirs = new StorageDir[storageDirNum];
    long quotaBytes = 0;
    for (int i = 0; i < storageDirNum; i ++) {
      long storageDirId =
          StorageDirId.getStorageDirId(storageLevel, mStorageLevelAlias.getValue(), i);
      mStorageDirs[i] =
          new StorageDir(storageDirId, dirPaths[i], dirCapacitieBytes[i], dataFolder, userFolder,
              conf);
      quotaBytes += dirCapacitieBytes[i];
    }
    mCapacityBytes = quotaBytes;
    mNextStorageTier = nextTier;
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
    return mNextStorageTier;
  }

  /**
   * Get removed blocks of current StorageTier
   * 
   * @return Id list of removed blocks
   */
  public List<Long> getRemovedBlockIdList() {
    List<Long> removedBlockIds = new ArrayList<Long>();
    for (StorageDir dir : mStorageDirs) {
      dir.getRemovedBlockIdList().drainTo(removedBlockIds);
    }
    return removedBlockIds;
  };

  /**
   * Find the StorageDir for certain blockId
   * 
   * @param blockId the id of the block
   * @return StorageDir which contains the block
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
  }

  /**
   * Get StorageDir by array index
   * 
   * @param dirIndex index of the StorageDir
   * @return the chosen StorageDir, null if index out of boundary
   */
  public StorageDir getStorageDirByIndex(int dirIndex) {
    if (dirIndex < mStorageDirs.length && dirIndex >= 0) {
      return mStorageDirs[dirIndex];
    } else {
      return null;
    }
  }

  /**
   * Get StorageDirs in current storage tier
   * 
   * @return StorageDirs in current tier
   */
  public StorageDir[] getStorageDirs() {
    return mStorageDirs;
  }

  /**
   * Get StorageLevel of current StorageTier
   * 
   * @return StorageLevel of current storage tier
   */
  public int getStorageLevel() {
    return mStorageLevel;
  }

  /**
   * Get StorageLevelAlias of current StorageTier
   * 
   * @return StorageLevelAlias of current StorageTier
   */
  public StorageLevelAlias getStorageLevelAlias() {
    return mStorageLevelAlias;
  }

  /**
   * Get used space in current StorageTier
   * 
   * @return used space size
   */
  public long getUsedBytes() {
    long used = 0;
    for (StorageDir dir : mStorageDirs) {
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
    for (StorageDir dir : mStorageDirs) {
      dir.initailize();
    }
  }

  /**
   * Check whether current tier is the last tier
   * 
   * @return true if current tier is the last tier, false otherwise
   */
  public boolean isLastTier() {
    return mNextStorageTier == null;
  }

  /**
   * Request certain space from current StorageTier
   * 
   * @param userId id of the user
   * @param requestSize size to request
   * @param pinList pinned files
   * @return the StorageDir assigned.
   * @throws IOException
   */
  public StorageDir requestSpace(long userId, long requestSize, Set<Integer> pinList)
      throws IOException {
    return requestSpace(mStorageDirs, userId, requestSize, pinList);
  }

  /**
   * Request certain space from current StorageTier
   * 
   * @param storageDir StorageDir that the space will be allocated in
   * @param userId id of the user
   * @param requestSize size to request
   * @param pinList pinned files
   * @return true if allocate successfully, false otherwise.
   * @throws IOException
   */
  public boolean requestSpace(StorageDir storageDir, long userId, long requestSize,
      Set<Integer> pinList) throws IOException {
    if (StorageDirId.getStorageLevel(storageDir.getStorageDirId()) != mStorageLevel) {
      return false;
    }
    StorageDir[] dirCandidates = new StorageDir[1];
    dirCandidates[0] = storageDir;
    return storageDir == requestSpace(dirCandidates, userId, requestSize, pinList);
  }

  /**
   * Request certain space from current StorageTier
   * 
   * @param dirCandidates candidates of StorageDir that the space will be allocated in
   * @param userId id of the user
   * @param requestSize size to request
   * @param pinList pinned files
   * @return the StorageDir assigned.
   * @throws IOException
   */
  // TODO make block eviction asynchronous
  private StorageDir requestSpace(StorageDir[] dirCandidates, long userId, long requestSize,
      Set<Integer> pinList) throws IOException {
    StorageDir dirSelected = mSpaceAllocator.getStorageDir(dirCandidates, userId, requestSize);
    if (dirSelected != null) {
      return dirSelected;
    } else if (mSpaceAllocator.fitInPossible(dirCandidates, requestSize)) {
      for (int attempt = 0; attempt < mRequestSpaceMaxTryTimes; attempt ++) {
        Pair<StorageDir, List<BlockInfo>> evictInfo =
            mBlockEvictor.getDirCandidate(dirCandidates, pinList, requestSize);
        if (evictInfo == null) {
          return null;
        }
        dirSelected = evictInfo.getFirst();
        List<BlockInfo> blocksInfoList = evictInfo.getSecond();
        for (BlockInfo blockInfo : blocksInfoList) {
          StorageDir srcDir = blockInfo.getStorageDir();
          if (!srcDir.isBlockLocked(blockInfo.getBlockId())) { // pinList is not updated
            long blockId = blockInfo.getBlockId();
            if (isLastTier()) {
              srcDir.deleteBlock(blockId);
            } else if (mNextStorageTier.containsBlock(blockId)) {
              srcDir.deleteBlock(blockId);
            } else {
              StorageDir dstDir =
                  mNextStorageTier.requestSpace(userId, blockInfo.getBlockSize(), pinList);
              srcDir.moveBlock(blockId, dstDir);
            }
          }
        }
        if (dirSelected.requestSpace(userId, requestSize)) {
          return dirSelected;
        } else {
          LOG.warn("Request space attempt failed! attempt time:" + attempt + " storage level:"
              + mStorageLevel);
        }
      }
    }
    throw new IOException("No StorageDir is allocated!");
  }

  @Override
  public String toString() {
    return mStorageLevel + "_" + mStorageLevelAlias;
  }
}
