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

package tachyon.worker.tiered;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.Users;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.worker.BlockHandler;
import tachyon.worker.SpaceCounter;
import tachyon.worker.WorkerSource;

/**
 * Stores and manages block files in storage's directory in different storage systems.
 */
public final class StorageDir {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final long NON_EXISTENT_TIME = -1L;
  /** Mapping from blockId to blockSize in bytes */
  private final ConcurrentMap<Long, Long> mBlockSizes = new ConcurrentHashMap<Long, Long>();
  /** Mapping from blockId to its last access time in milliseconds */
  private final ConcurrentMap<Long, Long> mLastBlockAccessTimeMs =
      new ConcurrentHashMap<Long, Long>();
  /** List of added block Ids to report */
  private final BlockingQueue<Long> mAddedBlockIdList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  /** Set of block Ids to remove */
  private final Set<Long> mToRemoveBlockIdSet = Collections.synchronizedSet(new HashSet<Long>());
  /** Space counter of the StorageDir */
  private final SpaceCounter mSpaceCounter;
  /** Id of StorageDir */
  private final long mStorageDirId;
  /** Root path of the StorageDir */
  private final TachyonURI mDirPath;
  /** Path of the data in the StorageDir */
  private final TachyonURI mDataPath;
  /** Path of user temporary directory in the StorageDir */
  private final TachyonURI mUserTempPath;
  /** Under file system of the current StorageDir */
  private final UnderFileSystem mFs;
  /** Configuration of under file system */
  private final Object mConf;
  /** Mapping from user Id to space size bytes owned by the user */
  private final ConcurrentMap<Long, Long> mOwnBytesPerUser = new ConcurrentHashMap<Long, Long>();
  /** Mapping from temporary block id to the size bytes allocated to it */
  private final ConcurrentMap<Pair<Long, Long>, Long> mTempBlockAllocatedBytes =
      new ConcurrentHashMap<Pair<Long, Long>, Long>();
  /** Mapping from user Id to list of blocks locked by the user */
  private final Multimap<Long, Long> mLockedBlocksPerUser = Multimaps
      .synchronizedMultimap(HashMultimap.<Long, Long>create());
  /** Mapping from block Id to list of users that lock the block */
  private final Multimap<Long, Long> mUserPerLockedBlock = Multimaps
      .synchronizedMultimap(HashMultimap.<Long, Long>create());
  /** TachyonConf for this StorageDir */
  private final TachyonConf mTachyonConf;
  /** The WorkerSource instance in the metrics system */
  private final WorkerSource mWorkerSource;

  /**
   * Create a new StorageDir.
   *
   * @param storageDirId id of StorageDir
   * @param dirPath root path of StorageDir
   * @param capacityBytes capacity of StorageDir in bytes
   * @param dataFolder data folder in current StorageDir
   * @param userTempFolder temporary folder for users in current StorageDir
   * @param conf configuration of under file system
   * @param tachyonConf the TachyonConf instance of the under file system
   * @param workerSource the WorkerSource instance in the metrics system
   */
  StorageDir(long storageDirId, String dirPath, long capacityBytes, String dataFolder,
      String userTempFolder, Object conf, TachyonConf tachyonConf, WorkerSource workerSource) {
    mTachyonConf = tachyonConf;
    mStorageDirId = storageDirId;
    mDirPath = new TachyonURI(dirPath);
    mSpaceCounter = new SpaceCounter(capacityBytes);
    mDataPath = mDirPath.join(dataFolder);
    mUserTempPath = mDirPath.join(userTempFolder);
    mConf = conf;
    mFs = UnderFileSystem.get(dirPath, conf, mTachyonConf);
    mWorkerSource = workerSource;
  }

  /**
   * Update the last access time of the block
   *
   * @param blockId Id of the block
   */
  public void accessBlock(long blockId) {
    synchronized (mLastBlockAccessTimeMs) {
      if (containsBlock(blockId)) {
        mLastBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
        mWorkerSource.incBlocksAccessed();
      }
    }
  }

  /**
   * Adds a block into the StorageDir.
   *
   * @param blockId the Id of the block
   * @param sizeBytes the size of the block in bytes
   * @param report if true, report to the master with the heartbeat
   */
  private void addBlockId(long blockId, long sizeBytes, boolean report) {
    addBlockId(blockId, sizeBytes, System.currentTimeMillis(), report);
  }

  /**
   * Adds a block into the StorageDir.
   *
   * @param blockId Id of the block
   * @param sizeBytes size of the block in bytes
   * @param accessTimeMs access time of the block in milliseconds
   * @param report if true, report to the master with the heartbeat
   */
  private void addBlockId(long blockId, long sizeBytes, long accessTimeMs, boolean report) {
    synchronized (mLastBlockAccessTimeMs) {
      mLastBlockAccessTimeMs.put(blockId, accessTimeMs);
      if (mBlockSizes.containsKey(blockId)) {
        mSpaceCounter.returnUsedBytes(mBlockSizes.remove(blockId));
      }
      mBlockSizes.put(blockId, sizeBytes);
      if (report) {
        mAddedBlockIdList.add(blockId);
      }
    }
  }

  /**
   * Move the cached block file from user temporary folder to data folder
   *
   * @param userId the id of the user
   * @param blockId the id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean cacheBlock(long userId, long blockId) throws IOException {
    String srcPath = getUserTempFilePath(userId, blockId);
    String dstPath = getBlockFilePath(blockId);
    Pair<Long, Long> blockInfo = new Pair<Long, Long>(userId, blockId);

    if (!(mFs.exists(srcPath) && mTempBlockAllocatedBytes.containsKey(blockInfo))) {
      cancelBlock(userId, blockId);
      throw new IOException("Block file doesn't exist! blockId:" + blockId + " " + srcPath);
    }
    long blockSize = mFs.getFileSize(srcPath);
    if (blockSize < 0) {
      cancelBlock(userId, blockId);
      throw new IOException("Negative block size! blockId:" + blockId);
    }
    Long allocatedBytes = mTempBlockAllocatedBytes.remove(blockInfo);
    returnSpace(userId, allocatedBytes - blockSize);
    if (mFs.rename(srcPath, dstPath)) {
      addBlockId(blockId, blockSize, false);
      updateUserOwnBytes(userId, -blockSize);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Cancel a block which is being written
   *
   * @param userId the id of the user
   * @param blockId the id of the block to be cancelled
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean cancelBlock(long userId, long blockId) throws IOException {
    String filePath = getUserTempFilePath(userId, blockId);
    Long allocatedBytes = mTempBlockAllocatedBytes.remove(new Pair<Long, Long>(userId, blockId));
    if (allocatedBytes == null) {
      allocatedBytes = 0L;
    }
    returnSpace(userId, allocatedBytes);
    mWorkerSource.incBlocksCanceled();
    if (!mFs.exists(filePath)) {
      return true;
    } else {
      return mFs.delete(filePath, false);
    }
  }

  /**
   * Clean resources related to the removed user
   *
   * @param userId id of the removed user
   * @param tempBlockIdList list of block ids that are being written by the user
   */
  public void cleanUserResources(long userId, Collection<Long> tempBlockIdList) {
    Collection<Long> blockIds = mLockedBlocksPerUser.removeAll(userId);
    for (long blockId : blockIds) {
      mUserPerLockedBlock.remove(blockId, userId);
    }
    for (Long tempBlockId : tempBlockIdList) {
      mTempBlockAllocatedBytes.remove(new Pair<Long, Long>(userId, tempBlockId));
    }
    try {
      mFs.delete(getUserTempPath(userId), true);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
    returnSpace(userId);
  }

  /**
   * Check whether the StorageDir contains a certain block
   *
   * @param blockId Id of the block
   * @return true if StorageDir contains the block, false otherwise
   */
  public boolean containsBlock(long blockId) {
    return mLastBlockAccessTimeMs.containsKey(blockId);
  }

  /**
   * Copy block file from this StorageDir to another StorageDir, the caller must ensure that this
   * block is locked during copying
   *
   * @param blockId Id of the block
   * @param dstDir destination StorageDir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean copyBlock(long blockId, StorageDir dstDir) throws IOException {
    long size = getBlockSize(blockId);
    if (size == -1) {
      LOG.error("Block file doesn't exist! blockId:{}", blockId);
      return false;
    }
    boolean copySuccess = false;
    Closer closer = Closer.create();
    ByteBuffer buffer = null;
    BlockHandler bhDst = null;
    try {
      BlockHandler bhSrc = closer.register(getBlockHandler(blockId));
      bhDst = closer.register(dstDir.getBlockHandler(blockId));
      buffer = bhSrc.read(0, (int) size);
      copySuccess = (bhDst.append(0, buffer) == size);
    } finally {
      closer.close();
      CommonUtils.cleanDirectBuffer(buffer);
    }
    if (copySuccess) {
      Long accessTimeMs = mLastBlockAccessTimeMs.get(blockId);
      if (accessTimeMs != null) {
        dstDir.addBlockId(blockId, size, accessTimeMs, true);
      } else {
        // The block had been freed during our copy. Because we lock the block before copy, the
        // actual block file is not deleted but the blockId is deleted from mLastBlockAccessTimeMs.
        // So we delete the copied block and return the space. We still think copyBlock is
        // successful and return true as nothing needed to be copied.
        bhDst.delete();
        dstDir.returnSpace(Users.MIGRATE_DATA_USER_ID, size);
      }
    }
    return copySuccess;
  }

  /**
   * Remove a block from the current StorageDir. Once this method is called, this block will no
   * longer be available.
   *
   * @param blockId Id of the block to remove.
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean deleteBlock(long blockId) throws IOException {
    Long accessTimeMs = mLastBlockAccessTimeMs.remove(blockId);
    if (accessTimeMs == null) {
      LOG.warn("Block does not exist in current StorageDir! blockId:{}", blockId);
      return false;
    }
    String blockFile = getBlockFilePath(blockId);
    // Should check lock status here
    if (!isBlockLocked(blockId)) {
      if (!mFs.delete(blockFile, false)) {
        LOG.error("Failed to delete block file! filename:{}", blockFile);
        return false;
      }
      deleteBlockId(blockId);
    } else {
      mToRemoveBlockIdSet.add(blockId);
      LOG.debug("Add block file {} to remove list!", blockFile);
    }
    return true;
  }

  /**
   * Delete the information of a block from the current StorageDir.
   *
   * @param blockId Id of the block
   */
  private void deleteBlockId(long blockId) {
    synchronized (mLastBlockAccessTimeMs) {
      mLastBlockAccessTimeMs.remove(blockId);
      mSpaceCounter.returnUsedBytes(mBlockSizes.remove(blockId));
      if (mAddedBlockIdList.contains(blockId)) {
        mAddedBlockIdList.remove(blockId);
      }
      mWorkerSource.incBlocksDeleted();
    }
  }

  /**
   * Get Ids of newly added blocks
   *
   * @return list of added block Ids
   */
  public List<Long> getAddedBlockIdList() {
    List<Long> addedBlockIdList = new ArrayList<Long>();
    mAddedBlockIdList.drainTo(addedBlockIdList);
    return addedBlockIdList;
  }

  /**
   * Get the size of available space in bytes in the current StorageDir
   *
   * @return available space size in current StorageDir
   */
  public long getAvailableBytes() {
    return mSpaceCounter.getAvailableBytes();
  }

  /**
   * Read data into ByteBuffer from some block file, caller must ensure that the block is locked
   * during reading
   *
   * @param blockId Id of the block
   * @param offset starting position of the block file
   * @param length length of data to read
   * @return ByteBuffer which contains data of the block
   * @throws IOException
   */
  public ByteBuffer getBlockData(long blockId, long offset, int length) throws IOException {
    BlockHandler bh = getBlockHandler(blockId);
    try {
      return bh.read(offset, length);
    } finally {
      bh.close();
      accessBlock(blockId);
    }
  }

  /**
   * Get file path of the block file
   *
   * @param blockId Id of the block
   * @return file path of the block
   */
  public String getBlockFilePath(long blockId) {
    return mDataPath.join(String.valueOf(blockId)).toString();
  }

  /**
   * Get block handler used to access the block file
   *
   * @param blockId Id of the block
   * @return block handler of the block file
   * @throws IOException
   */
  public BlockHandler getBlockHandler(long blockId) throws IOException {
    String filePath = getBlockFilePath(blockId);
    try {
      return BlockHandler.get(filePath);
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Get Ids of the blocks in current StorageDir
   *
   * @return Ids of the blocks in current StorageDir
   */
  public Set<Long> getBlockIds() {
    return mLastBlockAccessTimeMs.keySet();
  }

  /**
   * Get size of the block in bytes
   *
   * @param blockId Id of the block
   * @return size of the block, -1 if block doesn't exist
   */
  public long getBlockSize(long blockId) {
    Long size = mBlockSizes.get(blockId);
    if (size == null) {
      return -1;
    } else {
      return size;
    }
  }

  /**
   * Get sizes of the blocks in bytes in current StorageDir
   *
   * @return set of map entry mapping from block Id to the block size in current StorageDir
   */
  public Set<Entry<Long, Long>> getBlockSizes() {
    return mBlockSizes.entrySet();
  }

  /**
   * Get capacity of current StorageDir in bytes
   *
   * @return capacity of current StorageDir in bytes
   */
  public long getCapacityBytes() {
    return mSpaceCounter.getCapacityBytes();
  }

  /**
   * Get data path of current StorageDir
   *
   * @return data path of current StorageDir
   */
  public TachyonURI getDirDataPath() {
    return mDataPath;
  }

  /**
   * Get root path of current StorageDir
   *
   * @return root path of StorageDir
   */
  public TachyonURI getDirPath() {
    return mDirPath;
  }

  /**
   * Get last access time of blocks in current StorageDir
   *
   * @return set of map entry mapping from block Id to its last access time in current StorageDir
   */
  public Set<Entry<Long, Long>> getLastBlockAccessTimeMs() {
    return mLastBlockAccessTimeMs.entrySet();
  }

  /**
   * Get last access time of a block in current StorageDir
   *
   * @return the last block access time in Ms, -1 if the block doesn't exist
   */
  public long getLastBlockAccessTimeMs(long blockId) {
    Long lastBlockAccessTimeMs = mLastBlockAccessTimeMs.get(blockId);
    return lastBlockAccessTimeMs != null ? lastBlockAccessTimeMs : NON_EXISTENT_TIME;
  }

  /**
   * Get size of locked blocks in bytes in current StorageDir
   *
   * @return size of locked blocks in bytes in current StorageDir
   */
  public long getLockedSizeBytes() {
    long lockedBytes = 0;
    synchronized (mUserPerLockedBlock) {
      for (long blockId : mUserPerLockedBlock.keySet()) {
        Long blockSize = mBlockSizes.get(blockId);
        if (blockSize != null) {
          lockedBytes += blockSize;
        }
      }
    }
    return lockedBytes;
  }

  /**
   * Get the number of blocks in this StorageDir
   *
   * @return the number of blocks in this StorageDir
   */
  public int getNumberOfBlocks() {
    return mBlockSizes.size();
  }

  /**
   * Get Id of current StorageDir
   *
   * @return Id of current StorageDir
   */
  public long getStorageDirId() {
    return mStorageDirId;
  }

  /**
   * Get current StorageDir's under file system
   *
   * @return StorageDir's under file system
   */
  public UnderFileSystem getUfs() {
    return mFs;
  }

  /**
   * Get configuration of current StorageDir's under file system
   *
   * @return configuration of the under file system
   */
  public Object getUfsConf() {
    return mConf;
  }

  /**
   * Get used space in bytes in current StorageDir
   *
   * @return used space in bytes in current StorageDir
   */
  public long getUsedBytes() {
    return mSpaceCounter.getUsedBytes();
  }

  /**
   * Get temporary space owned by the user in current StorageDir
   *
   * @return temporary space in bytes owned by the user in current StorageDir
   */
  public long getUserOwnBytes(long userId) {
    Long ownBytes = mOwnBytesPerUser.get(userId);
    if (ownBytes == null) {
      ownBytes = 0L;
    }
    return ownBytes;
  }

  /**
   * Get temporary file path of block written by some user
   *
   * @param userId Id of the user
   * @param blockId Id of the block
   * @return temporary file path of the block
   */
  public String getUserTempFilePath(long userId, long blockId) {
    return mUserTempPath.join(String.valueOf(userId)).join(String.valueOf(blockId)).toString();
  }

  /**
   * Get root temporary path of users
   *
   * @return TachyonURI of users' temporary path
   */
  public TachyonURI getUserTempPath() {
    return mUserTempPath;
  }

  /**
   * Get temporary path of some user
   *
   * @param userId Id of the user
   * @return temporary path of the user
   */
  public String getUserTempPath(long userId) {
    return mUserTempPath.join(String.valueOf(userId)).toString();
  }

  /**
   * Initialize current StorageDir
   *
   * @throws IOException
   */
  public void initialize() throws IOException {
    String dataPath = mDataPath.toString();
    if (!mFs.exists(dataPath)) {
      LOG.info("Data folder {} does not exist. Creating a new one.", mDataPath);
      mFs.mkdirs(dataPath, true);
      mFs.setPermission(dataPath, "775");
    } else if (mFs.isFile(dataPath)) {
      String msg = "Data folder " + mDataPath + " is not a folder!";
      throw new IllegalArgumentException(msg);
    }

    String userTempPath = mUserTempPath.toString();
    if (!mFs.exists(userTempPath)) {
      LOG.info("User temp folder {} does not exist. Creating a new one.", mUserTempPath);
      mFs.mkdirs(userTempPath, true);
      mFs.setPermission(userTempPath, "775");
    } else if (mFs.isFile(userTempPath)) {
      String msg = "User temp folder " + mUserTempPath + " is not a folder!";
      throw new IllegalArgumentException(msg);
    }

    int cnt = 0;
    for (String name : mFs.list(dataPath)) {
      String path = mDataPath.join(name).toString();
      if (mFs.isFile(path)) {
        cnt ++;
        long fileSize = mFs.getFileSize(path);
        LOG.debug("File {}: {} with size {} Bs.", cnt, path, fileSize);
        long blockId = getBlockIdFromFileName(name);
        boolean success = mSpaceCounter.requestSpaceBytes(fileSize);
        if (success) {
          addBlockId(blockId, fileSize, true);
        } else {
          mFs.delete(path, true);
          LOG.warn("Pre-existing files exceed storage capacity. deleting file:{}", path);
        }
      }
    }
  }

  private long getBlockIdFromFileName(String name) {
    long fileId;
    try {
      fileId = Long.parseLong(name);
    } catch (Exception e) {
      throw new IllegalArgumentException("Wrong file name: " + name);
    }
    return fileId;
  }

  /**
   * Check whether a certain block is locked
   *
   * @param blockId Id of the block
   * @return true if block is locked, false otherwise
   */
  public boolean isBlockLocked(long blockId) {
    return mUserPerLockedBlock.containsKey(blockId);
  }

  /**
   * Lock block by some user
   *
   * @param blockId Id of the block
   * @param userId Id of the user
   * @return true if success, false otherwise
   */
  public boolean lockBlock(long blockId, long userId) {
    synchronized (mLastBlockAccessTimeMs) {
      if (!containsBlock(blockId)) {
        return false;
      }
      mUserPerLockedBlock.put(blockId, userId);
      mLockedBlocksPerUser.put(userId, blockId);
      return true;
    }
  }

  /**
   * Move a block from its current StorageDir to another StorageDir
   *
   * @param blockId the id of the block
   * @param dstDir the destination StorageDir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean moveBlock(long blockId, StorageDir dstDir) throws IOException {
    if (lockBlock(blockId, Users.MIGRATE_DATA_USER_ID)) {
      boolean result = false;
      try {
        result = copyBlock(blockId, dstDir);
      } finally {
        unlockBlock(blockId, Users.MIGRATE_DATA_USER_ID);
      }
      if (result) {
        return deleteBlock(blockId);
      }
    }
    return false;
  }

  /**
   * Request a specific amount of space from the current StorageDir by a user.
   *
   * @param userId Id of the user
   * @param size request size in bytes
   * @return true if success, false otherwise
   */
  public boolean requestSpace(long userId, long size) {
    boolean result = mSpaceCounter.requestSpaceBytes(size);
    if (result && userId != Users.MIGRATE_DATA_USER_ID) {
      updateUserOwnBytes(userId, size);
    }
    return result;
  }

  /**
   * Return space owned by the user to current StorageDir
   *
   * @param userId Id of the user
   */
  private void returnSpace(long userId) {
    Long ownBytes = mOwnBytesPerUser.remove(userId);
    if (ownBytes != null) {
      mSpaceCounter.returnUsedBytes(ownBytes);
    }
  }

  /**
   * Return space to current StorageDir by some user
   *
   * @param userId Id of the user
   * @param size size to return in bytes
   */
  public void returnSpace(long userId, long size) {
    mSpaceCounter.returnUsedBytes(size);
    updateUserOwnBytes(userId, -size);
  }

  /**
   * Unlock block by some user
   *
   * @param blockId Id of the block
   * @param userId Id of the user
   * @return true if success, false otherwise
   */
  public boolean unlockBlock(long blockId, long userId) {
    if (!mUserPerLockedBlock.remove(blockId, userId)) {
      return false;
    }
    if (!mLockedBlocksPerUser.remove(userId, blockId)) {
      // NOTE: we are not expected to reach here.
      // TODO Recover from this tricky case where mUserPerLockedBlock has been updated.
      LOG.error("Cannot remove userId from mLockedBlocksPerUser. userId:{}", blockId);
      return false;
    }
    // Check if this blocks is currently hold by other users.
    if (isBlockLocked(blockId) || !mToRemoveBlockIdSet.contains(blockId)) {
      return true;
    }
    try {
      if (!mFs.delete(getBlockFilePath(blockId), false)) {
        return false;
      }
      mToRemoveBlockIdSet.remove(blockId);
      deleteBlockId(blockId);
      return true;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  /**
   * Update allocated space bytes of a temporary block in current StorageDir
   *
   * @param userId Id of the user
   * @param blockId Id of the block
   * @param sizeBytes updated space size in bytes
   */
  public void updateTempBlockAllocatedBytes(long userId, long blockId, long sizeBytes) {
    Pair<Long, Long> blockInfo = new Pair<Long, Long>(userId, blockId);
    Long oldSize = mTempBlockAllocatedBytes.putIfAbsent(blockInfo, sizeBytes);
    if (oldSize != null) {
      while (!mTempBlockAllocatedBytes.replace(blockInfo, oldSize, oldSize + sizeBytes)) {
        oldSize = mTempBlockAllocatedBytes.get(blockInfo);
        if (oldSize == null) {
          LOG.error("Temporary block doesn't exist! blockId:{}", blockId);
          break;
        }
      }
    }
  }

  /**
   * Update user owned space bytes
   *
   * @param userId Id of the user
   * @param sizeBytes updated space size in bytes
   */
  private void updateUserOwnBytes(long userId, long sizeBytes) {
    Long used = mOwnBytesPerUser.putIfAbsent(userId, sizeBytes);
    if (used == null) {
      return;
    }
    while (!mOwnBytesPerUser.replace(userId, used, used + sizeBytes)) {
      used = mOwnBytesPerUser.get(userId);
      if (used == null) {
        LOG.error("Unknown user! userId:{}", userId);
        break;
      }
    }
  }
}
