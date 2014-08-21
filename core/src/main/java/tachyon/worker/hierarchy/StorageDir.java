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
import tachyon.client.BlockHandlerLocal;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerSpaceCounter;

/**
 * It is used to store and manage block files in storage directory on different
 * under file systems.
 */
public class StorageDir {
  protected final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final Set<Long> BLOCK_IDS = new HashSet<Long>();
  private final Map<Long, Long> BLOCK_SIZES = new HashMap<Long, Long>();
  private final String DATA_PATH;
  private final Map<Long, Long> LAST_BLOCK_ACCESS_TIME_MS = new HashMap<Long, Long>();

  private final Map<Long, Set<Long>> LOCKED_BLOCKS_PER_USER = new HashMap<Long, Set<Long>>();
  private final BlockingQueue<Long> REMOVED_BLOCK_LIST = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  private final WorkerSpaceCounter SPACE_COUNTER;
  private final String DIR_PATH;
  private final long STORAGE_ID;
  private final UnderFileSystem UFS;
  private final Object UFS_CONF;
  private final Map<Long, Long> USER_ALLOCATED_SPACE = new HashMap<Long, Long>();
  private final String USER_TEMP_PATH;
  private final Map<Long, Set<Long>> USER_PER_LOCKED_BLOCK = new HashMap<Long, Set<Long>>();

  StorageDir(long storageId, String dirPath, long capacity, String dataFolder,
      String userTempFolder, Object conf) {
    DIR_PATH = dirPath;
    UFS_CONF = conf;
    UFS = UnderFileSystem.get(DIR_PATH, conf);
    SPACE_COUNTER = new WorkerSpaceCounter(capacity);
    STORAGE_ID = storageId;
    DATA_PATH = DIR_PATH + Constants.PATH_SEPARATOR + dataFolder;
    USER_TEMP_PATH = DIR_PATH + Constants.PATH_SEPARATOR + userTempFolder;
  }

  /**
   * Update the access time of the block
   * 
   * @param blockId
   *          id of the block
   */
  public void accessBlock(long blockId) {
    synchronized (LAST_BLOCK_ACCESS_TIME_MS) {
      LAST_BLOCK_ACCESS_TIME_MS.put(blockId, System.currentTimeMillis());
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
    synchronized (LAST_BLOCK_ACCESS_TIME_MS) {
      LAST_BLOCK_ACCESS_TIME_MS.put(blockId, System.currentTimeMillis());
      BLOCK_SIZES.put(blockId, size);
      BLOCK_IDS.add(blockId);
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
    boolean result = UFS.rename(srcPath, destPath);
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
      synchronized (USER_PER_LOCKED_BLOCK) {
        Set<Long> blockIds = LOCKED_BLOCKS_PER_USER.get(userId);
        LOCKED_BLOCKS_PER_USER.remove(userId);
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
    return BLOCK_IDS.contains(blockId);
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
    if ((bhSrc instanceof BlockHandlerLocal) && (bhDst instanceof BlockHandlerLocal)) {
      copySuccess =
          ((BlockHandlerLocal) bhSrc).readChannel().transferTo(0, len,
              ((BlockHandlerLocal) bhDst).writeChannel()) > 0;
    } else {
      byte[] blockData = new byte[len];
      ByteBuffer bf = bhSrc.readByteBuffer(0, len);
      bf.get(blockData);
      copySuccess = bhDst.appendCurrentBuffer(blockData, 0, 0, len) > 0;
    }
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
    synchronized (LAST_BLOCK_ACCESS_TIME_MS) {
      if (BLOCK_IDS.contains(blockId)) {
        String blockfile = getBlockFilePath(blockId);
        boolean result = UFS.delete(blockfile, true);
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
    synchronized (LAST_BLOCK_ACCESS_TIME_MS) {
      LAST_BLOCK_ACCESS_TIME_MS.remove(blockId);
      returnSpace(BLOCK_SIZES.remove(blockId));
      BLOCK_IDS.remove(blockId);
      REMOVED_BLOCK_LIST.add(blockId);

    }
  }

  /**
   * Get available space in current storage dir
   * 
   * @return available space in current storage dir
   */
  public long getAvailable() {
    return SPACE_COUNTER.getAvailableBytes();
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
    return DATA_PATH + Constants.PATH_SEPARATOR + blockId;
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
   * Get ids of the blocks on current storage dir
   * 
   * @return ids of the blocks
   */
  public Set<Long> getBlockIds() {
    return BLOCK_IDS;
  }

  /**
   * Get size of the block
   * 
   * @param blockId
   *          id of the block
   * @return size of the block
   */
  public long getBlockSize(long blockId) {
    if (BLOCK_SIZES.containsKey(blockId)) {
      return BLOCK_SIZES.get(blockId);
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
    return BLOCK_SIZES;
  }

  /**
   * Get capacity of current storage dir
   * 
   * @return capacity of current storage dir
   */
  public long getCapacity() {
    return SPACE_COUNTER.getCapacityBytes();
  }

  /**
   * Get data path on current storage dir
   * 
   * @return data path on current storage dir
   */
  public String getDirDataPath() {
    return DATA_PATH;
  }

  /**
   * Get path of current storage dir
   * 
   * @return path of storage dir
   */
  public String getDirPath() {
    return DIR_PATH;
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
    return UFS.getFileSize(blockfile);
  }

  /**
   * Get access time of blocks
   * 
   * @return access time of blocks
   */
  public Map<Long, Long> getLastBlockAccessTime() {
    return LAST_BLOCK_ACCESS_TIME_MS;
  }

  /**
   * Get removed block ids
   * 
   * @return removed block ids
   */
  public List<Long> getRemovedBlockList() {
    List<Long> removedBlockList = new ArrayList<Long>();
    while (REMOVED_BLOCK_LIST.size() > 0) {
      removedBlockList.add(REMOVED_BLOCK_LIST.poll());
    }
    return removedBlockList;
  }

  /**
   * Get storage id of current storage dir
   * 
   * @return storage id of current storage dir
   */
  public long getStorageId() {
    return STORAGE_ID;
  }

  /**
   * Get configuration of current dir's under file system
   * 
   * @return configuration of the under file system
   */
  public UnderFileSystem getUfs() {
    return UFS;
  }

  /**
   * Get configuration of current dir's under file system
   * 
   * @return configuration of the under file system
   */
  public Object getUfsConf() {
    return UFS_CONF;
  }

  /**
   * Get used space on current storage dir
   * 
   * @return used space on current storage dir
   */
  public long getUsed() {
    return SPACE_COUNTER.getUsedBytes();
  }

  /**
   * Get users of locked blocks
   * 
   * @return users of locked blocks
   */
  public Map<Long, Set<Long>> getUsersPerLockedBlock() {
    return USER_PER_LOCKED_BLOCK;
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
    return USER_TEMP_PATH + Constants.PATH_SEPARATOR + userId + Constants.PATH_SEPARATOR + blockId;
  }

  /**
   * Initailize the storage dir
   * 
   * @throws IOException
   */
  public void initailize() throws IOException {
    if (!UFS.exists(DATA_PATH)) {
      LOG.info("Local folder " + DATA_PATH + " does not exist. Creating a new one.");
      UFS.mkdirs(DATA_PATH, true);
      UFS.mkdirs(USER_TEMP_PATH, true);
      UFS.setPermission(DATA_PATH, "775");
      UFS.setPermission(USER_TEMP_PATH, "775");
      return;
    }

    if (UFS.isFile(DATA_PATH)) {
      String tmp = "Data folder " + DATA_PATH + " is not a folder!";
      LOG.error(tmp);
      throw new IllegalArgumentException(tmp);
    }

    int cnt = 0;
    for (String name : UFS.list(DATA_PATH)) {
      String path = DATA_PATH + Constants.PATH_SEPARATOR + name;
      if (UFS.isFile(path)) {
        cnt ++;
        long fileSize = UFS.getFileSize(path);
        LOG.info("File " + cnt + ": " + path + " with size " + fileSize + " Bs.");
        long blockId = CommonUtils.getBlockIdFromFileName(name);
        boolean success = SPACE_COUNTER.requestSpaceBytes(fileSize);// no user
        if (success) {
          addBlockId(blockId, fileSize);
        } else {
          UFS.delete(path, true);
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
    if (!BLOCK_IDS.contains(blockId)) {
      return;
    }
    synchronized (USER_PER_LOCKED_BLOCK) {
      if (!USER_PER_LOCKED_BLOCK.containsKey(blockId)) {
        USER_PER_LOCKED_BLOCK.put(blockId, new HashSet<Long>());
      }
      USER_PER_LOCKED_BLOCK.get(blockId).add(userId);
      if (!LOCKED_BLOCKS_PER_USER.containsKey(userId)) {
        LOCKED_BLOCKS_PER_USER.put(userId, new HashSet<Long>());
      }
      LOCKED_BLOCKS_PER_USER.get(userId).add(blockId);
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
    boolean result = SPACE_COUNTER.requestSpaceBytes(size);
    if (result) {
      if (USER_ALLOCATED_SPACE.containsKey(userId)) {
        long current = USER_ALLOCATED_SPACE.get(userId);
        USER_ALLOCATED_SPACE.put(userId, current + size);
      } else {
        USER_ALLOCATED_SPACE.put(userId, size);
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
    SPACE_COUNTER.returnUsedBytes(size);
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
    SPACE_COUNTER.returnUsedBytes(size);
    if (USER_ALLOCATED_SPACE.containsKey(userId)) {
      long current = USER_ALLOCATED_SPACE.get(userId);
      USER_ALLOCATED_SPACE.put(userId, current - size);
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
    if (!BLOCK_IDS.contains(blockId)) {
      return;
    }
    synchronized (USER_PER_LOCKED_BLOCK) {
      if (USER_PER_LOCKED_BLOCK.containsKey(blockId)) {
        USER_PER_LOCKED_BLOCK.get(blockId).remove(userId);
        if (USER_PER_LOCKED_BLOCK.get(blockId).size() == 0) {
          USER_PER_LOCKED_BLOCK.remove(blockId);
        }
      }
      if (LOCKED_BLOCKS_PER_USER.containsKey(userId)) {
        LOCKED_BLOCKS_PER_USER.get(userId).remove(blockId);
      }
    }
  }
}
