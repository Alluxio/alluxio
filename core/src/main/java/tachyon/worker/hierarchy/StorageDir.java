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
 * it is used to store and manage block files in storage directory on different
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

  public StorageDir(long storageId, String dirPath, long capacity, String dataFolder,
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
   * update the access time of the block
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
   * add a block id in current storage dir
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
   * move the cached file from user temp directory to data directory
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
   * check status of the users
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
   * check whether current storage dir contains certain block
   * 
   * @param blockId
   *          id of the block
   * @return true if contains, false otherwise
   */
  public boolean containsBlock(long blockId) {
    return mBlockIds.contains(blockId);
  }

  /**
   * copy block from current storage dir to another
   * 
   * @param blockId
   *          id of the block
   * @param dst
   *          Destiny storage dir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean copyBlock(long blockId, StorageDir dst) throws IOException {
    BlockHandler bhSrc = getBlockHandler(blockId);
    int len = (int) getBlockSize(blockId);
    ByteBuffer bf = bhSrc.readByteBuffer(0, len);
    bhSrc.close();
    try {
      byte[] blockData = new byte[len];
      bf.get(blockData);
      String filePath = dst.getBlockFilePath(blockId);
      BlockHandler bhDst = BlockHandler.get(filePath, mUFSConf);
      boolean copySuccess = bhDst.appendCurrentBuffer(blockData, 0, 0, len) > 0;
      if (copySuccess) {
        dst.addBlockId(blockId, len);
      }
      bhDst.close();
      return copySuccess;
    } catch (IllegalArgumentException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Remove a block from the memory.
   * 
   * @param blockId
   *          The block to be removed.
   * @return Removed file size in bytes.
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
          LOG.warn("error during delete block! blockfile:" + blockfile);
        }
        return result;
      } else {
        LOG.warn("File " + blockId + " does not exist in memory.");
        return false;
      }
    }
  }

  /**
   * delete block from current storage dir
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
   * get available space on current storage dir
   * 
   * @return available space on current storage dir
   */
  public long getAvailable() {
    return mSpaceCounter.getAvailableBytes();
  }

  /**
   * get data of the block file
   * 
   * @param blockId
   *          id of the block
   * @param offset
   *          offset of the file
   * @param length
   *          length of data to read
   * @return bytebuffer contains data of the block
   * @throws IOException
   */
  public ByteBuffer getBlockData(long blockId, long offset, long length) throws IOException {
    BlockHandler bh = getBlockHandler(blockId);
    ByteBuffer bf = bh.readByteBuffer((int) offset, (int) length);
    return bf;
  }

  /**
   * get file path of the block
   * 
   * @param blockId
   *          id of the block
   * @return file path of the block
   */
  public String getBlockFilePath(long blockId) {
    return mDataPath + Constants.PATH_SEPARATOR + blockId;
  }

  /**
   * get block handler of the block
   * 
   * @param blockId
   *          id of the block
   * @return block handler of the block
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
   * get ids of the blocks on current storage dir
   * 
   * @return ids of the blocks
   */
  public Set<Long> getBlockIds() {
    return mBlockIds;
  }

  /**
   * get size of the block
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
   * get sizes of the blocks on current storage dir
   * 
   * @return sizes of the blocks
   * @throws IOException
   */
  public Map<Long, Long> getBlockSizes() throws IOException {
    return mBlockSizes;
  }

  /**
   * get capacity of current storage dir
   * 
   * @return capacity of current storage dir
   */
  public long getCapacity() {
    return mSpaceCounter.getCapacityBytes();
  }

  /**
   * get data path on current storage dir
   * 
   * @return data path on current storage dir
   */
  public String getDirDataPath() {
    return mDataPath;
  }

  /**
   * get path of current storage dir
   * 
   * @return path of storage dir
   */
  public String getDirPath() {
    return mDirPath;
  }

  /**
   * get size of the block file on current storage dir
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
   * get access time of blocks
   * 
   * @return access time of blocks
   */
  public Map<Long, Long> getLastBlockAccessTime() {
    return mLatestBlockAccessTimeMs;
  }

  /**
   * get removed block ids
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
   * get storage id of current storage dir
   * 
   * @return storage id of current storage dir
   */
  public long getStorageId() {
    return mStorageId;
  }

  /**
   * get configuration of current dir's under file system
   * 
   * @return configuration of the under file system
   */
  public Object getUfsConf() {
    return mUFSConf;
  }

  /**
   * get used space on current storage dir
   * 
   * @return used space on current storage dir
   */
  public long getUsed() {
    return mSpaceCounter.getUsedBytes();
  }

  /**
   * get users of locked blocks
   * 
   * @return users of locked blocks
   */
  public Map<Long, Set<Long>> getUsersPerLockedBlock() {
    return mUsersPerLockedBlock;
  }

  /**
   * get temp file path for block written by some user
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
   * initailize the storage dir
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
   * lock block by the user
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
   * move block from current storage dir to another storage dir
   * 
   * @param blockId
   *          id of the block
   * @param dst
   *          destiny storage dir
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean moveBlock(long blockId, StorageDir dst) throws IOException {
    boolean copySuccess = false;
    copySuccess = copyBlock(blockId, dst);
    if (copySuccess) {
      return deleteBlock(blockId);
    } else {
      return false;
    }
  }

  /**
   * allocate space from current storage dir
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
   * return space to current storage dir
   * 
   * @param size
   *          size to return
   */
  public void returnSpace(long size) {
    mSpaceCounter.returnUsedBytes(size);
  }

  /**
   * return space to current storage dir by some user
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
      LOG.warn("error during return space: unknown user ID");
    }
  }

  /**
   * unlock block by some user
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
