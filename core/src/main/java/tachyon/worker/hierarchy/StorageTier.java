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
 * successor StorageTier to get enough space requested. Each StorageTier may contains several
 * StorageDirs. It recommends to configure multiple StorageDirs in each StorageTier, to spread out
 * the I/O for better performance.
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
  private static final int FAILED_SPACE_REQUEST_LIMITS = UserConf.get().FAILED_SPACE_REQUEST_LIMITS;

  /**
   * Create a new StorageTier
   * 
   * @param storageLevel storage level of StorageTier
   * @param storageLevelAlias alias of current StorageTier's storage level
   * @param dirPaths paths of StorageDirs in current StorageTier
   * @param dirCapacityBytes capacities of StorageDirs in current StorageTier
   * @param dataFolder data folder in StorageDir
   * @param userTempFolder user temporary folder in StorageDir
   * @param nextTier the successor StorageTier
   * @param conf configuration of StorageDir
   * @throws IOException
   */
  public StorageTier(int storageLevel, StorageLevelAlias storageLevelAlias, String[] dirPaths,
      long[] dirCapacityBytes, String dataFolder, String userTempFolder, StorageTier nextTier,
      Object conf) throws IOException {
    mStorageLevel = storageLevel;
    int storageDirNum = dirPaths.length;
    mStorageLevelAlias = storageLevelAlias;
    mStorageDirs = new StorageDir[storageDirNum];
    long quotaBytes = 0;
    for (int i = 0; i < storageDirNum; i ++) {
      long storageDirId =
          StorageDirId.getStorageDirId(storageLevel, mStorageLevelAlias.getValue(), i);
      mStorageDirs[i] =
          new StorageDir(storageDirId, dirPaths[i], dirCapacityBytes[i], dataFolder,
              userTempFolder, conf);
      quotaBytes += dirCapacityBytes[i];
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
   * Find the StorageDir which contains given block Id
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
   * @return StorageDir selected, null if index out of boundary
   */
  public StorageDir getStorageDirByIndex(int dirIndex) {
    if (dirIndex < mStorageDirs.length && dirIndex >= 0) {
      return mStorageDirs[dirIndex];
    } else {
      return null;
    }
  }

  /**
   * Get StorageDirs in current StorageTier
   * 
   * @return StorageDirs in current StorageTier
   */
  public StorageDir[] getStorageDirs() {
    return mStorageDirs;
  }

  /**
   * Get storage level of current StorageTier
   * 
   * @return storage level of current StorageTier
   */
  public int getStorageLevel() {
    return mStorageLevel;
  }

  /**
   * Get alias of current StorageTier's storage level
   * 
   * @return alias of current StorageTier's storage level
   */
  public StorageLevelAlias getStorageLevelAlias() {
    return mStorageLevelAlias;
  }

  /**
   * Get used space in current StorageTier
   * 
   * @return used space size in bytes
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
   * Check whether current StorageTier is the last tier
   * 
   * @return true if current StorageTier is the last tier, false otherwise
   */
  public boolean isLastTier() {
    return mNextStorageTier == null;
  }

  /**
   * Request space from current StorageTier by some user
   * 
   * @param userId id of the user
   * @param requestSizeBytes size to request in bytes
   * @param pinList list of pinned files
   * @return the StorageDir assigned.
   * @throws IOException
   */
  public StorageDir requestSpace(long userId, long requestSizeBytes, Set<Integer> pinList)
      throws IOException {
    return requestSpace(mStorageDirs, userId, requestSizeBytes, pinList);
  }

  /**
   * Request space from current StorageTier by some user
   * 
   * @param storageDir StorageDir that the space will be allocated in
   * @param userId id of the user
   * @param requestSizeBytes size to request in bytes
   * @param pinList list of pinned files
   * @return true if allocate successfully, false otherwise.
   * @throws IOException
   */
  public boolean requestSpace(StorageDir storageDir, long userId, long requestSizeBytes,
      Set<Integer> pinList) throws IOException {
    if (StorageDirId.getStorageLevel(storageDir.getStorageDirId()) != mStorageLevel) {
      return false;
    }
    StorageDir[] dirCandidates = new StorageDir[1];
    dirCandidates[0] = storageDir;
    return storageDir == requestSpace(dirCandidates, userId, requestSizeBytes, pinList);
  }

  /**
   * Request space from current StorageTier by some user
   * 
   * @param dirCandidates candidates of StorageDir that the space will be allocated in
   * @param userId id of the user
   * @param requestSizeBytes size to request in bytes
   * @param pinList list of pinned files
   * @return the StorageDir assigned.
   * @throws IOException
   */
  // TODO make block eviction asynchronous
  private StorageDir requestSpace(StorageDir[] dirCandidates, long userId, long requestSizeBytes,
      Set<Integer> pinList) throws IOException {
    StorageDir dirSelected = mSpaceAllocator.getStorageDir(dirCandidates, userId, requestSizeBytes);
    if (dirSelected != null) {
      return dirSelected;
    } else if (mSpaceAllocator.fitInPossible(dirCandidates, requestSizeBytes)) {
      for (int attempt = 0; attempt < FAILED_SPACE_REQUEST_LIMITS; attempt ++) {
        Pair<StorageDir, List<BlockInfo>> evictInfo =
            mBlockEvictor.getDirCandidate(dirCandidates, pinList, requestSizeBytes);
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
        if (dirSelected.requestSpace(userId, requestSizeBytes)) {
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
