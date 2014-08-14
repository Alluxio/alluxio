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

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.client.BlockHandler;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerSpaceCounter;

/**
 * It is used to store and manage block files in storage directory on different
 * under file systems.
 */
public class StorageDir {
  protected final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final Set<Long> mBlockIds = new HashSet<Long>();
  private final Map<Long, Long> mBlockSizes = new HashMap<Long, Long>();
  private final String mDataPath;
  private final Map<Long, Long> mLatestBlockAccessTimeMs = new HashMap<Long, Long>();

  private final Map<Long, Set<Long>> mLockedBlocksPerUser = new HashMap<Long, Set<Long>>();
  private final BlockingQueue<Long> mRemovedBlockList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  private final WorkerSpaceCounter mSpaceCounter;
  private final String mDirPath;
  private final long mStorageId;
  private final UnderFileSystem mUFS;
  private final Object mUFSConf;
  private final Map<Long, Long> mUserAllocatedSpace = new HashMap<Long, Long>();
  private final String mUserTempPath;
  private final Map<Long, Set<Long>> mUsersPerLockedBlock = new HashMap<Long, Set<Long>>();

  StorageDir(long storageId, String dirPath, long capacity, String dataFolder,
      String userTempFolder, Object conf) {
    mDirPath = dirPath;
    mUFSConf = conf;
    mUFS = UnderFileSystem.get(mDirPath, conf);
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
    synchronized (mLatestBlockAccessTimeMs) {
      mLatestBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
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
    synchronized (mLatestBlockAccessTimeMs) {
      mLatestBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
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
    boolean result = mUFS.rename(srcPath, destPath);
    if (result) {
      addBlockId(blockId, getFileSize(blockId));
    }
    return result;
  }

  /**
   * Check status of the users
   * 
   * @param removedUsers
   *          id of the removed users
   */
  public void checkStatus(List<Long> removedUsers) {
    for (long userId : removedUsers) {
      synchronized (mUsersPerLockedBlock) {
        Set<Long> blockIds = mLockedBlocksPerUser.get(userId);
        mLockedBlocksPerUser.remove(userId);
        if (blockIds != null) {
          for (long blockId : blockIds) {
            unlockBlock(blockId, userId);
          }
        }
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
    return mBlockIds.contains(blockId);
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
    int len = (int) getBlockSize(blockId);
    ByteBuffer bf = bhSrc.readByteBuffer(0, len);
    bhSrc.close();
    try {
      byte[] blockData = new byte[len];
      bf.get(blockData);
      String filePath = dstDir.getBlockFilePath(blockId);
      BlockHandler bhDst = BlockHandler.get(filePath, mUFSConf);
      boolean copySuccess = bhDst.appendCurrentBuffer(blockData, 0, 0, len) > 0;
      if (copySuccess) {
        dstDir.addBlockId(blockId, len);
      }
      bhDst.close();
      return copySuccess;
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage());
    }
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
    synchronized (mLatestBlockAccessTimeMs) {
      if (mBlockIds.contains(blockId)) {
        String blockfile = getBlockFilePath(blockId);
        boolean result = mUFS.delete(blockfile, true);
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
    synchronized (mLatestBlockAccessTimeMs) {
      mLatestBlockAccessTimeMs.remove(blockId);
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
    ByteBuffer bf = bh.readByteBuffer((int) offset, (int) length);
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
    return mDataPath + Constants.PATH_SEPARATOR + blockId;
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
    String filePath = CommonUtils.concat(this.getDirDataPath(), blockId + "");
    try {
      return BlockHandler.get(filePath, mUFSConf);
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Get ids of the blocks on current storage dir
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
    if (mBlockSizes.containsKey(blockId)) {
      return mBlockSizes.get(blockId);
    } else {
      return -1;
    }
  }

  /**
   * Get sizes of the blocks on current storage dir
   * 
   * @return sizes of the blocks
   * @throws IOException
   */
  public Map<Long, Long> getBlockSizes() throws IOException {
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
    return mUFS.getFileSize(blockfile);
  }

  /**
   * Get access time of blocks
   * 
   * @return access time of blocks
   */
  public Map<Long, Long> getLastBlockAccessTime() {
    return mLatestBlockAccessTimeMs;
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
  public Object getUfsConf() {
    return mUFSConf;
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
  public Map<Long, Set<Long>> getUsersPerLockedBlock() {
    return mUsersPerLockedBlock;
  }

  /**
   * Get temp file path for block written by some user
   * 
   * @param userId
   *          id of the user
   * @param blockId
   *          id of the block
   * @return path of the temp file
   */
  public String getUserTempFilePath(long userId, long blockId) {
    return mUserTempPath + Constants.PATH_SEPARATOR + userId + Constants.PATH_SEPARATOR + blockId;
  }

  /**
   * Initailize the storage dir
   * 
   * @throws IOException
   */
  public void initailize() throws IOException {
    if (!mUFS.exists(mDataPath)) {
      LOG.info("Local folder " + mDataPath + " does not exist. Creating a new one.");
      mUFS.mkdirs(mDataPath, true);
      mUFS.mkdirs(mUserTempPath, true);
      mUFS.setPermission(mDataPath, "775");
      mUFS.setPermission(mUserTempPath, "775");
      return;
    }

    if (mUFS.isFile(mDataPath)) {
      String tmp = "Data folder " + mDataPath + " is not a folder!";
      LOG.error(tmp);
      throw new IllegalArgumentException(tmp);
    }

    int cnt = 0;
    for (String name : mUFS.list(mDataPath)) {
      String path = mDataPath + Constants.PATH_SEPARATOR + name;
      if (mUFS.isFile(path)) {
        cnt ++;
        long fileSize = mUFS.getFileSize(path);
        LOG.info("File " + cnt + ": " + path + " with size " + fileSize + " Bs.");
        long blockId = CommonUtils.getBlockIdFromFileName(name);
        boolean success = mSpaceCounter.requestSpaceBytes(fileSize);// no user
        if (success) {
          addBlockId(blockId, fileSize);
        } else {
          mUFS.delete(path, true);
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
    if (!mBlockIds.contains(blockId)) {
      return;
    }
    synchronized (mUsersPerLockedBlock) {
      if (!mUsersPerLockedBlock.containsKey(blockId)) {
        mUsersPerLockedBlock.put(blockId, new HashSet<Long>());
      }
      mUsersPerLockedBlock.get(blockId).add(userId);
      if (!mLockedBlocksPerUser.containsKey(userId)) {
        mLockedBlocksPerUser.put(userId, new HashSet<Long>());
      }
      mLockedBlocksPerUser.get(userId).add(blockId);
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
    boolean copySuccess = false;
    copySuccess = copyBlock(blockId, dstDir);
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
    boolean result = mSpaceCounter.requestSpaceBytes(size);
    if (result) {
      if (mUserAllocatedSpace.containsKey(userId)) {
        long current = mUserAllocatedSpace.get(userId);
        mUserAllocatedSpace.put(userId, current + size);
      } else {
        mUserAllocatedSpace.put(userId, size);
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
    mSpaceCounter.returnUsedBytes(size);
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
    mSpaceCounter.returnUsedBytes(size);
    if (mUserAllocatedSpace.containsKey(userId)) {
      long current = mUserAllocatedSpace.get(userId);
      mUserAllocatedSpace.put(userId, current - size);
    } else {
      LOG.warn("Error during return space: unknown user ID");
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
    if (!mBlockIds.contains(blockId)) {
      return;
    }
    synchronized (mUsersPerLockedBlock) {
      if (mUsersPerLockedBlock.containsKey(blockId)) {
        mUsersPerLockedBlock.get(blockId).remove(userId);
        if (mUsersPerLockedBlock.get(blockId).size() == 0) {
          mUsersPerLockedBlock.remove(blockId);
        }
      }
      if (mLockedBlocksPerUser.containsKey(userId)) {
        mLockedBlocksPerUser.get(userId).remove(blockId);
      }
    }
  }
}
