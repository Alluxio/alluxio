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
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.Users;
import tachyon.util.CommonUtils;
import tachyon.worker.BlockHandler;
import tachyon.worker.SpaceCounter;

/**
 * Used to store and manage block files in storage's directory on different under file systems.
 */
public final class StorageDir {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Mapping from blockId to blockSize in bytes */
  private final ConcurrentMap<Long, Long> mBlockSizes = new ConcurrentHashMap<Long, Long>();
  /** Mapping from blockId to its last access time in milliseconds */
  private final ConcurrentMap<Long, Long> mLastBlockAccessTimeMs =
      new ConcurrentHashMap<Long, Long>();
  /** List of added block Ids to be reported */
  private final BlockingQueue<Long> mAddedBlockIdList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  /** List of to be removed block Ids */
  private final Set<Long> mToRemoveBlockIdSet = Collections.synchronizedSet(new HashSet<Long>());
  /** Space counter of current StorageDir */
  private final SpaceCounter mSpaceCounter;
  /** Id of StorageDir */
  private final long mStorageDirId;
  /** Path of the data in current StorageDir */
  private final TachyonURI mDataPath;
  /** Root path of the current StorageDir */
  private final TachyonURI mDirPath;
  /** Path of user temporary directory in current StorageDir */
  private final TachyonURI mUserTempPath;
  /** Under file system of current StorageDir */
  private final UnderFileSystem mFs;
  /** Configuration of under file system */
  private final Object mConf;
  /** Mapping from user Id to available space in bytes */
  private final ConcurrentMap<Long, Long> mUserAvailableBytes = new ConcurrentHashMap<Long, Long>();
  /** Mapping from user Id to list of blocks locked by the user */
  private final Multimap<Long, Long> mLockedBlocksPerUser = Multimaps
      .synchronizedMultimap(HashMultimap.<Long, Long>create());
  /** Mapping from block Id to list of users that lock the block */
  private final Multimap<Long, Long> mUserPerLockedBlock = Multimaps
      .synchronizedMultimap(HashMultimap.<Long, Long>create());

  /**
   * Create a new StorageDir.
   * 
   * @param storageDirId id of StorageDir
   * @param dirPath root path of StorageDir
   * @param capacityBytes capacity of StorageDir in bytes
   * @param dataFolder data folder in current StorageDir
   * @param userTempFolder temporary folder for users in current StorageDir
   * @param conf configuration of under file system
   */
  StorageDir(long storageDirId, String dirPath, long capacityBytes, String dataFolder,
      String userTempFolder, Object conf) {
    mDirPath = new TachyonURI(dirPath);
    mConf = conf;
    mFs = UnderFileSystem.get(dirPath, conf);
    mSpaceCounter = new SpaceCounter(capacityBytes);
    mStorageDirId = storageDirId;
    mDataPath = mDirPath.join(dataFolder);
    mUserTempPath = mDirPath.join(userTempFolder);
  }

  /**
   * Update the last access time of the block
   * 
   * @param blockId Id of the block
   */
  public void accessBlock(long blockId) {
    if (containsBlock(blockId)) {
      mLastBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
    }
  }

  /**
   * Add information of a block in current StorageDir
   * 
   * @param blockId Id of the block
   * @param sizeBytes size of the block in bytes
   * @param need to be reported During heart beat with master
   */
  private void addBlockId(long blockId, long sizeBytes, boolean report) {
    addBlockId(blockId, sizeBytes, System.currentTimeMillis(), report);
  }

  /**
   * Add information of a block in current StorageDir
   * 
   * @param blockId Id of the block
   * @param sizeBytes size of the block in bytes
   * @param accessTime access time of the block
   * @param need to be reported During heart beat with master
   */
  private void addBlockId(long blockId, long sizeBytes, long accessTime, boolean report) {
    mLastBlockAccessTimeMs.put(blockId, accessTime);
    if (mBlockSizes.containsKey(blockId)) {
      mSpaceCounter.returnUsedBytes(mBlockSizes.remove(blockId));
    }
    mBlockSizes.put(blockId, sizeBytes);
    if (report) {
      mAddedBlockIdList.add(blockId);
    }
  }

  /**
   * Move the cached block file from user temporary directory to data directory
   * 
   * @param userId Id of the user
   * @param blockId Id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean cacheBlock(long userId, long blockId) throws IOException {
    String srcPath = getUserTempFilePath(userId, blockId);
    String dstPath = getBlockFilePath(blockId);
    long blockSize = mFs.getFileSize(srcPath);
    if (blockSize < 0) {
      throw new IOException("Negative size of block! size:" + blockSize);
    }
    boolean result = mFs.rename(srcPath, dstPath);
    if (result) {
      addBlockId(blockId, blockSize, false);
      Long used;
      do {
        used = mUserAvailableBytes.get(userId);
        if (used == null) {
          LOG.error("Failed to record allocated space for user! Id:" + userId);
          break;
        }
      } while (!mUserAvailableBytes.replace(userId, used, used - blockSize));
    }
    return result;
  }

  /**
   * Check status of the users, removedUsers can't be modified any more after being passed down from
   * the caller
   * 
   * @param removedUsers list of the removed users
   */
  public void checkStatus(List<Long> removedUsers) {
    for (long userId : removedUsers) {
      Collection<Long> blockIds = mLockedBlocksPerUser.removeAll(userId);
      for (long blockId : blockIds) {
        mUserPerLockedBlock.remove(blockId, userId);
      }
      returnSpace(userId);
    }
  }

  /**
   * Check whether current StorageDir contains certain block
   * 
   * @param blockId Id of the block
   * @return true if StorageDir contains the block, false otherwise
   */
  public boolean containsBlock(long blockId) {
    return mLastBlockAccessTimeMs.containsKey(blockId) || isBlockLocked(blockId);
  }

  /**
   * Copy block file from current StorageDir to another StorageDir
   * 
   * @param blockId Id of the block
   * @param dstDir destination StorageDir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean copyBlock(long blockId, StorageDir dstDir) throws IOException {
    long size = getBlockSize(blockId);
    if (size == -1) {
      LOG.error("Block file doesn't exist! blockId:" + blockId);
      return false;
    }
    boolean copySuccess = false;
    Closer closer = Closer.create();
    try {
      BlockHandler bhSrc = closer.register(getBlockHandler(blockId));
      BlockHandler bhDst = closer.register(dstDir.getBlockHandler(blockId));
      ByteBuffer srcBuf = bhSrc.read(0, (int) size);
      copySuccess = (bhDst.append(0, srcBuf) == size);
    } finally {
      closer.close();
    }
    if (copySuccess) {
      long accessTime = mLastBlockAccessTimeMs.get(blockId);
      dstDir.addBlockId(blockId, size, accessTime, true);
    }
    return copySuccess;
  }

  /**
   * Remove a block from current StorageDir
   * 
   * @param blockId Id of the block to be removed.
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public boolean deleteBlock(long blockId) throws IOException {
    Long accessTimeMs = mLastBlockAccessTimeMs.remove(blockId);
    if (accessTimeMs != null) {
      String blockfile = getBlockFilePath(blockId);
      boolean result = false;
      try {
        // Should check lock status here 
        if (!isBlockLocked(blockId)) {
          result = mFs.delete(blockfile, true);
        } else {
          mToRemoveBlockIdSet.add(blockId);
        }
      } finally {
        if (result) {
          deleteBlockId(blockId);
          LOG.debug("Removed block file:" + blockfile);
        } else {
          mLastBlockAccessTimeMs.put(blockId, accessTimeMs);
          LOG.error("Failed to delete block file! file name:" + blockfile);
        }
      }
      return result;
    } else {
      LOG.warn("Block " + blockId + " does not exist in current StorageDir.");
      return false;
    }
  }

  /**
   * Delete information of a block from current StorageDir
   * 
   * @param blockId Id of the block
   */
  private void deleteBlockId(long blockId) {
    mLastBlockAccessTimeMs.remove(blockId);
    mSpaceCounter.returnUsedBytes(mBlockSizes.remove(blockId));
    if (mAddedBlockIdList.contains(blockId)) {
      mAddedBlockIdList.remove(blockId);
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
   * Get available space size in bytes in current StorageDir
   * 
   * @return available space size in current StorageDir
   */
  public long getAvailableBytes() {
    return mSpaceCounter.getAvailableBytes();
  }

  /**
   * Read data into ByteBuffer from some block file
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
    return mDataPath.join("" + blockId).toString();
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
   * Get size of locked blocks in bytes in current StorageDir
   * 
   * @return size of locked blocks in bytes in current StorageDir
   */
  public long getLockedSizeBytes() {
    long lockedBytes = 0;
    for (long blockId : mUserPerLockedBlock.keySet()) {
      Long blockSize = mBlockSizes.get(blockId);
      if (blockSize != null) {
        lockedBytes += blockSize;
      }
    }
    return lockedBytes;
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
   * Get temporary file path of block written by some user
   * 
   * @param userId Id of the user
   * @param blockId Id of the block
   * @return temporary file path of the block
   */
  String getUserTempFilePath(long userId, long blockId) {
    return mUserTempPath.join("" + userId).join("" + blockId).toString();
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
    return mUserTempPath.join("" + userId).toString();
  }

  /**
   * Initialize current StorageDir
   * 
   * @throws IOException
   */
  public void initailize() throws IOException {
    String dataPath = mDataPath.toString();
    if (!mFs.exists(dataPath)) {
      LOG.info("Data folder " + mDataPath + " does not exist. Creating a new one.");
      mFs.mkdirs(dataPath, true);
      mFs.setPermission(dataPath, "775");
    } else if (mFs.isFile(dataPath)) {
      String msg = "Data folder " + mDataPath + " is not a folder!";
      throw new IllegalArgumentException(msg);
    }

    String userTempPath = mUserTempPath.toString();
    if (!mFs.exists(userTempPath)) {
      LOG.info("User temp folder " + mUserTempPath + " does not exist. Creating a new one.");
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
        LOG.debug("File " + cnt + ": " + path + " with size " + fileSize + " Bs.");
        long blockId = CommonUtils.getBlockIdFromFileName(name);
        boolean success = mSpaceCounter.requestSpaceBytes(fileSize);
        if (success) {
          addBlockId(blockId, fileSize, true);
        } else {
          mFs.delete(path, true);
          throw new RuntimeException("Pre-existing files exceed storage capacity.");
        }
      }
    }
    return;
  }

  /**
   * Check whether certain block is locked
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
    if (!containsBlock(blockId)) {
      return false;
    }
    mUserPerLockedBlock.put(blockId, userId);
    mLockedBlocksPerUser.put(userId, blockId);
    return true;
  }

  /**
   * Move block file from current StorageDir to another StorageDir
   * 
   * @param blockId Id of the block
   * @param dstDir destination StorageDir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean moveBlock(long blockId, StorageDir dstDir) throws IOException {
    boolean copySuccess = copyBlock(blockId, dstDir);
    if (copySuccess) {
      return deleteBlock(blockId);
    } else {
      return false;
    }
  }

  /**
   * Request space from current StorageDir by some user
   * 
   * @param userId Id of the user
   * @param size request size in bytes
   * @return true if success, false otherwise
   */
  public boolean requestSpace(long userId, long size) {
    boolean result = mSpaceCounter.requestSpaceBytes(size);
    if (result && userId != Users.EVICT_USER_ID) {
      Long used = mUserAvailableBytes.putIfAbsent(userId, size);
      if (used != null) {
        while (!mUserAvailableBytes.replace(userId, used, used + size)) {
          used = mUserAvailableBytes.get(userId);
          if (used == null) {
            LOG.error("Failed to request space! unknown user Id:" + userId);
            break;
          }
        }
      }
    }
    return result;
  }

  /**
   * Return space to current StorageDir
   * 
   * @param userId Id of the user to return space
   */
  private void returnSpace(long userId) {
    Long returnedSize = mUserAvailableBytes.remove(userId);
    if (returnedSize != null) {
      LOG.info(String.format("Return space(%d) from removed user(%d):", returnedSize, userId));
      mSpaceCounter.returnUsedBytes(returnedSize);
    } else {
      LOG.warn("No space allocation record for removed user! Id:" + userId);
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
    Long used;
    do {
      used = mUserAvailableBytes.get(userId);
      if (used == null) {
        LOG.error("Failed to return space! unknown user Id:" + userId);
        break;
      }
    } while (!mUserAvailableBytes.replace(userId, used, used - size));
  }

  /**
   * Unlock block by some user
   * 
   * @param blockId Id of the block
   * @param userId Id of the user
   * @return true if success, false otherwise
   */
  public boolean unlockBlock(long blockId, long userId) {
    if (!containsBlock(blockId)) {
      return false;
    }
    mUserPerLockedBlock.remove(blockId, userId);
    mLockedBlocksPerUser.remove(userId, blockId);
    if (!mUserPerLockedBlock.containsKey(blockId) && mToRemoveBlockIdSet.contains(blockId)) {
      try {
        if (deleteBlock(blockId)) {
          mToRemoveBlockIdSet.remove(blockId);
        }
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }
    return true;
  }
}
