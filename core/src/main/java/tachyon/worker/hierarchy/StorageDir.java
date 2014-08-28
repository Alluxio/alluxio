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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.client.BlockHandler;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerSpaceCounter;

/**
 * It is used to store and manage block files in storage directory on different
 * under file systems.
 */
public final class StorageDir {
  protected final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final Set<Long> mBlockIds = new HashSet<Long>();
  private final Map<Long, Long> mBlockSizes = new HashMap<Long, Long>();
  private final String mDataPath;
  private final Map<Long, Long> mLastBlockAccessTimeMS = new HashMap<Long, Long>();
  private final HashMultimap<Long, Long> mLockedBlocksPerUser = HashMultimap.create();
  private final BlockingQueue<Long> mRemovedBlockList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  private final WorkerSpaceCounter mSpaceCounter;
  private final String mDirPath;
  private final long mStorageId;
  private final UnderFileSystem mUfs;
  private final Object mUfsConf;
  private final Map<Long, Long> mUserAllocatedSpace = new HashMap<Long, Long>();
  private final String mUserTempPath;
  private final HashMultimap<Long, Long> mUserPerLockedBlock = HashMultimap.create();

  StorageDir(long storageId, String dirPath, long capacity, String dataFolder,
      String userTempFolder, Object conf) {
    mDirPath = dirPath;
    mUfsConf = conf;
    mUfs = UnderFileSystem.get(mDirPath, conf);
    mSpaceCounter = new WorkerSpaceCounter(capacity);
    mStorageId = storageId;
    mDataPath = mDirPath + Constants.PATH_SEPARATOR + dataFolder;
    mUserTempPath = mDirPath + Constants.PATH_SEPARATOR + userTempFolder;
  }

  /**
   * Update the access time of the block
   * 
   * @param blockId
   *          id of the block
   */
  public void accessBlock(long blockId) {
    synchronized (this) {
      mLastBlockAccessTimeMS.put(blockId, System.currentTimeMillis());
    }
  }

  /**
   * Add a block id in current storage dir
   * 
   * @param blockId
   *          id of the block
   * @param size
   *          size of the block file
   */
  private void addBlockId(long blockId, long size) {
    synchronized (this) {
      mLastBlockAccessTimeMS.put(blockId, System.currentTimeMillis());
      mBlockSizes.put(blockId, size);
      mBlockIds.add(blockId);
    }
  }

  /**
   * Move the cached file from user temp directory to data directory
   * 
   * @param userId
   *          id of the user
   * @param blockId
   *          id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean cacheBlock(long userId, long blockId) throws IOException {
    String srcPath = getUserTempFilePath(userId, blockId);
    String destPath = getBlockFilePath(blockId);
    boolean result = mUfs.rename(srcPath, destPath);
    if (result) {
      addBlockId(blockId, getFileSize(blockId));
    }
    return result;
  }

  /**
   * Check status of the users, removed users are passed down from the caller, and can't be modified
   * any more
   * 
   * @param removedUsers
   *          id of the removed users
   */
  public void checkStatus(List<Long> removedUsers) {
    for (long userId : removedUsers) {
      synchronized (mUserPerLockedBlock) {
        Set<Long> blockIds = mLockedBlocksPerUser.get(userId);
        mLockedBlocksPerUser.removeAll(userId);
        if (blockIds != null) {
          for (long blockId : blockIds) {
            unlockBlock(blockId, userId);
          }
        }
      }
      synchronized (mUserAllocatedSpace) {
        mUserAllocatedSpace.remove(userId);
      }
    }
  }

  /**
   * Check whether current storage dir contains certain block
   * 
   * @param blockId
   *          id of the block
   * @return true if contains, false otherwise
   */
  public boolean containsBlock(long blockId) {
    synchronized (this) {
      return mBlockIds.contains(blockId);
    }
  }

  /**
   * Copy block from current storage dir to another
   * 
   * @param blockId
   *          id of the block
   * @param dstDir
   *          Destiny storage dir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean copyBlock(long blockId, StorageDir dstDir) throws IOException {
    BlockHandler bhSrc = getBlockHandler(blockId);
    BlockHandler bhDst = dstDir.getBlockHandler(blockId);
    int len = (int) getBlockSize(blockId);
    boolean copySuccess = false;

    ByteBuffer srcBuf = bhSrc.read(0, len);
    copySuccess = bhDst.append(0, srcBuf) > 0;
    bhSrc.close();
    bhDst.close();
    if (copySuccess) {
      dstDir.addBlockId(blockId, len);
    }
    return copySuccess;
  }

  /**
   * Remove a block from the memory
   * 
   * @param blockId
   *          The block to be removed.
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public boolean deleteBlock(long blockId) throws IOException {
    synchronized (this) {
      if (mBlockIds.contains(blockId)) {
        String blockfile = getBlockFilePath(blockId);
        boolean result = mUfs.delete(blockfile, true);
        if (result) {
          deleteBlockId(blockId);
          LOG.info("Removed Data " + blockId);
        } else {
          LOG.warn("Error during delete block! blockfile:" + blockfile);
        }
        return result;
      } else {
        LOG.warn("File " + blockId + " does not exist in memory.");
        return false;
      }
    }
  }

  /**
   * Delete block from current storage dir
   * 
   * @param blockId
   *          id of the block
   */
  private void deleteBlockId(long blockId) {
    synchronized (this) {
      mLastBlockAccessTimeMS.remove(blockId);
      returnSpace(mBlockSizes.remove(blockId));
      mBlockIds.remove(blockId);
      mRemovedBlockList.add(blockId);
    }
  }

  /**
   * Get available space in current storage dir
   * 
   * @return available space in current storage dir
   */
  public long getAvailable() {
    return mSpaceCounter.getAvailableBytes();
  }

  /**
   * Get data of the block file
   * 
   * @param blockId
   *          id of the block
   * @param offset
   *          offset of the file
   * @param length
   *          length of data to read
   * @return a ByteBuffer contains data of the block
   * @throws IOException
   */
  public ByteBuffer getBlockData(long blockId, long offset, long length) throws IOException {
    BlockHandler bh = getBlockHandler(blockId);
    ByteBuffer bf = bh.read((int) offset, (int) length);
    return bf;
  }

  /**
   * Get file path of the block
   * 
   * @param blockId
   *          id of the block
   * @return file path of the block
   */
  public String getBlockFilePath(long blockId) {
    return CommonUtils.concat(mDataPath, blockId);
  }

  /**
   * Get block handler of the block
   * 
   * @param blockId
   *          id of the block
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
   * Get ids of the blocks on current storage dir, caller must maintain the synchronization
   * 
   * @return ids of the blocks
   */
  public Set<Long> getBlockIds() {
    return mBlockIds;
  }

  /**
   * Get size of the block
   * 
   * @param blockId
   *          id of the block
   * @return size of the block
   */
  public long getBlockSize(long blockId) {
    synchronized (this) {
      if (mBlockSizes.containsKey(blockId)) {
        return mBlockSizes.get(blockId);
      } else {
        return -1;
      }
    }
  }

  /**
   * Get sizes of the blocks on current storage dir, caller must maintain the synchronization
   * 
   * @return sizes of the blocks
   * @throws IOException
   */
  public Map<Long, Long> getBlockSizes() {
    return mBlockSizes;
  }

  /**
   * Get capacity of current storage dir
   * 
   * @return capacity of current storage dir
   */
  public long getCapacity() {
    return mSpaceCounter.getCapacityBytes();
  }

  /**
   * Get data path on current storage dir
   * 
   * @return data path on current storage dir
   */
  public String getDirDataPath() {
    return mDataPath;
  }

  /**
   * Get path of current storage dir
   * 
   * @return path of storage dir
   */
  public String getDirPath() {
    return mDirPath;
  }

  /**
   * Get size of the block file on current storage dir
   * 
   * @param blockId
   *          id of the block
   * @return file size of the block
   * @throws IOException
   */
  private long getFileSize(long blockId) throws IOException {
    String blockfile = getBlockFilePath(blockId);
    return mUfs.getFileSize(blockfile);
  }

  /**
   * Get access time of blocks
   * 
   * @return access time of blocks
   */
  public Map<Long, Long> getLastBlockAccessTime() {
    return mLastBlockAccessTimeMS;
  }

  /**
   * Get removed block ids
   * 
   * @return removed block ids
   */
  public List<Long> getRemovedBlockList() {
    List<Long> removedBlockList = new ArrayList<Long>();
    while (mRemovedBlockList.size() > 0) {
      removedBlockList.add(mRemovedBlockList.poll());
    }
    return removedBlockList;
  }

  /**
   * Get storage id of current storage dir
   * 
   * @return storage id of current storage dir
   */
  public long getStorageId() {
    return mStorageId;
  }

  /**
   * Get configuration of current dir's under file system
   * 
   * @return configuration of the under file system
   */
  public UnderFileSystem getUfs() {
    return mUfs;
  }

  /**
   * Get configuration of current dir's under file system
   * 
   * @return configuration of the under file system
   */
  public Object getUfsConf() {
    return mUfsConf;
  }

  /**
   * Get used space on current storage dir
   * 
   * @return used space on current storage dir
   */
  public long getUsed() {
    return mSpaceCounter.getUsedBytes();
  }

  /**
   * Get users of locked blocks
   * 
   * @return users of locked blocks
   */
  public HashMultimap<Long, Long> getUsersPerLockedBlock() {
    return mUserPerLockedBlock;
  }

  /**
   * Get temp file path of block written by some user
   * 
   * @param userId
   *          id of the user
   * @param blockId
   *          id of the block
   * @return path of the temp file
   */
  public String getUserTempFilePath(long userId, long blockId) {
    return CommonUtils.concat(mUserTempPath, userId, blockId);
  }

  /**
   * Get temp folder path for some user
   * 
   * @param userId
   *          id of the user
   * @return path of the temp folder
   */
  public String getUserTempPath(long userId) {
    return CommonUtils.concat(mUserTempPath, userId);
  }

  /**
   * Initailize the storage dir
   * 
   * @throws IOException
   */
  public void initailize() throws IOException {
    if (!mUfs.exists(mDataPath)) {
      LOG.info("Local folder " + mDataPath + " does not exist. Creating a new one.");
      mUfs.mkdirs(mDataPath, true);
      mUfs.mkdirs(mUserTempPath, true);
      mUfs.setPermission(mDataPath, "775");
      mUfs.setPermission(mUserTempPath, "775");
      return;
    }

    if (mUfs.isFile(mDataPath)) {
      String tmp = "Data folder " + mDataPath + " is not a folder!";
      LOG.error(tmp);
      throw new IllegalArgumentException(tmp);
    }

    int cnt = 0;
    for (String name : mUfs.list(mDataPath)) {
      String path = mDataPath + Constants.PATH_SEPARATOR + name;
      if (mUfs.isFile(path)) {
        cnt ++;
        long fileSize = mUfs.getFileSize(path);
        LOG.info("File " + cnt + ": " + path + " with size " + fileSize + " Bs.");
        long blockId = CommonUtils.getBlockIdFromFileName(name);
        boolean success = mSpaceCounter.requestSpaceBytes(fileSize);// no user
        if (success) {
          addBlockId(blockId, fileSize);
        } else {
          mUfs.delete(path, true);
          throw new RuntimeException("Pre-existing files exceed the local memory capacity.");
        }
      }
    }
    return;
  }

  /**
   * Lock block by the user
   * 
   * @param blockId
   *          id of the block
   * @param userId
   *          id of the user
   */
  public void lockBlock(long blockId, long userId) {
    if (!containsBlock(blockId)) {
      return;
    }
    synchronized (mUserPerLockedBlock) {
      mUserPerLockedBlock.put(blockId, userId);
    }
    synchronized (mLockedBlocksPerUser) {
      mLockedBlocksPerUser.put(userId, blockId);
    }
  }

  /**
   * Move block from current storage dir to another storage dir
   * 
   * @param blockId
   *          id of the block
   * @param dstDir
   *          destiny storage dir
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
   * Allocate space from current storage dir
   * 
   * @param userId
   *          id of the user
   * @param size
   *          request size
   * @return true if sucess, false otherwise
   */
  public boolean requestSpace(long userId, long size) {
    boolean result = false;
    synchronized (mSpaceCounter) {
      result = mSpaceCounter.requestSpaceBytes(size);
    }
    if (result) {
      synchronized (mUserAllocatedSpace) {
        if (mUserAllocatedSpace.containsKey(userId)) {
          long current = mUserAllocatedSpace.get(userId);
          mUserAllocatedSpace.put(userId, current + size);
        } else {
          mUserAllocatedSpace.put(userId, size);
        }
      }
    }
    return result;
  }

  /**
   * Return space to current storage dir
   * 
   * @param size
   *          size to return
   */
  public void returnSpace(long size) {
    synchronized (mSpaceCounter) {
      mSpaceCounter.returnUsedBytes(size);
    }
  }

  /**
   * Return space to current storage dir by some user
   * 
   * @param userId
   *          id of the user
   * @param size
   *          size to return
   */
  public void returnSpace(long userId, long size) {
    returnSpace(size);
    synchronized (mUserAllocatedSpace) {
      if (mUserAllocatedSpace.containsKey(userId)) {
        long current = mUserAllocatedSpace.get(userId);
        mUserAllocatedSpace.put(userId, current - size);
      } else {
        LOG.warn("Error during return space: unknown user ID");
      }
    }
  }

  /**
   * Unlock block by some user
   * 
   * @param blockId
   *          id of the block
   * @param userId
   *          id of the user
   */
  public void unlockBlock(long blockId, long userId) {
    if (!containsBlock(blockId)) {
      return;
    }
    synchronized (mUserPerLockedBlock) {
      mUserPerLockedBlock.removeAll(blockId);
    }
    synchronized (mLockedBlocksPerUser) {
      mLockedBlocksPerUser.remove(userId, blockId);
    }
  }
}
