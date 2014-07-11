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
package tachyon.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.Users;
import tachyon.conf.CommonConf;
import tachyon.conf.WorkerConf;
import tachyon.master.BlockInfo;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.Command;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.util.CommonUtils;

/**
 * The structure to store a worker's information in worker node.
 */
public class WorkerStorage {
  /**
   * The CheckpointThread, used to checkpoint the files belong to the worker.
   */
  public class CheckpointThread implements Runnable {
    private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
    private final int ID;
    private UnderFileSystem mCheckpointUnderFs = null;

    public CheckpointThread(int id) {
      ID = id;
    }

    // This method assumes the mDependencyLock has been acquired.
    private int getFileIdBasedOnPriorityDependency() throws TException {
      if (mPriorityDependencies.isEmpty()) {
        return -1;
      }
      for (int depId : mPriorityDependencies) {
        int fileId = getFileIdFromOneDependency(depId);
        if (fileId != -1) {
          return fileId;
        }
      }
      return -1;
    }

    // This method assumes the mDependencyLock has been acquired.
    private int getFileIdFromOneDependency(int depId) throws TException {
      Set<Integer> fileIds = mDepIdToFiles.get(depId);
      if (fileIds != null && !fileIds.isEmpty()) {
        int fileId = fileIds.iterator().next();
        fileIds.remove(fileId);
        mUncheckpointFiles.remove(fileId);
        if (fileIds.isEmpty()) {
          mDepIdToFiles.remove(depId);
        }
        return fileId;
      }
      return -1;
    }

    // This method assumes the mDependencyLock has been acquired.
    private int getRandomUncheckpointedFile() throws TException {
      if (mUncheckpointFiles.isEmpty()) {
        return -1;
      }
      for (int depId : mDepIdToFiles.keySet()) {
        int fileId = getFileIdFromOneDependency(depId);
        if (fileId != -1) {
          return fileId;
        }
      }
      return -1;
    }

    private List<Integer> getSortedPriorityDependencyList() throws TException {
      List<Integer> ret = mMasterClient.worker_getPriorityDependencyList();
      for (int i = 0; i < ret.size(); i ++) {
        for (int j = i + 1; j < ret.size(); j ++) {
          if (ret.get(i) < ret.get(j)) {
            int k = ret.get(i);
            ret.set(i, ret.get(j));
            ret.set(j, k);
          }
        }
      }
      return ret;
    }

    @Override
    public void run() {
      while (true) {
        try {
          int fileId = -1;
          synchronized (mDependencyLock) {
            fileId = getFileIdBasedOnPriorityDependency();

            if (fileId == -1) {
              if (mPriorityDependencies.size() == 0) {
                mPriorityDependencies = getSortedPriorityDependencyList();
                if (!mPriorityDependencies.isEmpty()) {
                  LOG.info("Get new mPriorityDependencies "
                      + CommonUtils.listToString(mPriorityDependencies));
                }
              } else {
                List<Integer> tList = getSortedPriorityDependencyList();
                boolean equal = true;
                if (mPriorityDependencies.size() != tList.size()) {
                  equal = false;
                }
                if (equal) {
                  for (int k = 0; k < tList.size(); k ++) {
                    if (tList.get(k) != mPriorityDependencies.get(k)) {
                      equal = false;
                      break;
                    }
                  }
                }

                if (!equal) {
                  mPriorityDependencies = tList;
                }
              }

              fileId = getFileIdBasedOnPriorityDependency();
            }

            if (fileId == -1) {
              fileId = getRandomUncheckpointedFile();
            }
          }

          if (fileId == -1) {
            LOG.debug("Thread " + ID + " has nothing to checkpoint. Sleep for 1 sec.");
            CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
            continue;
          }

          // TODO checkpoint process. In future, move from midPath to dstPath should be done by
          // master
          String midPath = CommonUtils.concat(mUnderfsWorkerDataFolder, fileId);
          String dstPath = CommonUtils.concat(CommonConf.get().UNDERFS_DATA_FOLDER, fileId);
          LOG.info("Thread " + ID + " is checkpointing file " + fileId + " from "
              + mLocalDataFolder.toString() + " to " + midPath + " to " + dstPath);

          if (mCheckpointUnderFs == null) {
            mCheckpointUnderFs = UnderFileSystem.get(midPath);
          }

          long startCopyTimeMs = System.currentTimeMillis();
          ClientFileInfo fileInfo = mMasterClient.getClientFileInfoById(fileId);
          if (!fileInfo.isComplete) {
            LOG.error("File " + fileInfo + " is not complete!");
            continue;
          }
          for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
            lockBlock(fileInfo.blockIds.get(k), Users.sCHECKPOINT_USER_ID);
          }
          OutputStream os = mCheckpointUnderFs.create(midPath, (int) fileInfo.getBlockSizeByte());
          long fileSizeByte = 0;
          for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
            File tempFile =
                new File(CommonUtils.concat(mLocalDataFolder.toString(), fileInfo.blockIds.get(k)));
            fileSizeByte += tempFile.length();
            InputStream is = new FileInputStream(tempFile);
            byte[] buf = new byte[16 * Constants.KB];
            int got = is.read(buf);
            while (got != -1) {
              os.write(buf, 0, got);
              got = is.read(buf);
            }
            is.close();
          }
          os.close();
          if (!mCheckpointUnderFs.rename(midPath, dstPath)) {
            LOG.error("Failed to rename from " + midPath + " to " + dstPath);
          }
          mMasterClient.addCheckpoint(mWorkerId, fileId, fileSizeByte, dstPath);
          for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
            unlockBlock(fileInfo.blockIds.get(k), Users.sCHECKPOINT_USER_ID);
          }

          long shouldTakeMs =
              (long) (1000.0 * fileSizeByte / Constants.MB / WorkerConf.get().WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC);
          long currentTimeMs = System.currentTimeMillis();
          if (startCopyTimeMs + shouldTakeMs > currentTimeMs) {
            long shouldSleepMs = startCopyTimeMs + shouldTakeMs - currentTimeMs;
            LOG.info("Checkpointed last file " + fileId + " took "
                + (currentTimeMs - startCopyTimeMs) + " ms. Need to sleep " + shouldSleepMs
                + " ms.");
            CommonUtils.sleepMs(LOG, shouldSleepMs);
          }
        } catch (FileDoesNotExistException e) {
          LOG.warn(e);
        } catch (SuspectedFileSizeException e) {
          LOG.error(e);
        } catch (BlockInfoException e) {
          LOG.error(e);
        } catch (IOException e) {
          LOG.error(e);
        } catch (TException e) {
          LOG.warn(e);
        }
      }
    }
  }

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final CommonConf COMMON_CONF;
  private volatile MasterClient mMasterClient;
  private InetSocketAddress mMasterAddress;
  private InetSocketAddress mWorkerAddress;
  private WorkerSpaceCounter mWorkerSpaceCounter;

  private long mWorkerId;
  private Set<Long> mMemoryData = new HashSet<Long>();
  private Map<Long, Long> mBlockSizes = new HashMap<Long, Long>();

  private Map<Long, Long> mLatestBlockAccessTimeMs = new HashMap<Long, Long>();
  private Map<Long, Set<Long>> mUsersPerLockedBlock = new HashMap<Long, Set<Long>>();

  private Map<Long, Set<Long>> mLockedBlocksPerUser = new HashMap<Long, Set<Long>>();
  private BlockingQueue<Long> mRemovedBlockList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);

  private BlockingQueue<Long> mAddedBlockList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  private File mLocalDataFolder;
  private File mLocalUserFolder;
  private String mUnderfsWorkerFolder;
  private String mUnderfsWorkerDataFolder;
  private String mUnderfsOrphansFolder;

  private UnderFileSystem mUnderFs;

  private Users mUsers;
  // Dependency related lock
  private Object mDependencyLock = new Object();
  private Set<Integer> mUncheckpointFiles = new HashSet<Integer>();
  // From dependencyId to files in that set.
  private Map<Integer, Set<Integer>> mDepIdToFiles = new HashMap<Integer, Set<Integer>>();

  private List<Integer> mPriorityDependencies = new ArrayList<Integer>();

  private ArrayList<Thread> mCheckpointThreads = new ArrayList<Thread>(
      WorkerConf.get().WORKER_CHECKPOINT_THREADS);

  /**
   * @param masterAddress
   *          The TachyonMaster's address
   * @param workerAddress
   *          This TachyonWorker's address
   * @param dataFolder
   *          This TachyonWorker's local folder's path
   * @param memoryCapacityBytes
   *          The maximum memory space this TachyonWorker can use, in bytes
   */
  public WorkerStorage(InetSocketAddress masterAddress, InetSocketAddress workerAddress,
      String dataFolder, long memoryCapacityBytes) {
    COMMON_CONF = CommonConf.get();

    mMasterAddress = masterAddress;
    mMasterClient = new MasterClient(mMasterAddress);

    mWorkerAddress = workerAddress;
    mWorkerSpaceCounter = new WorkerSpaceCounter(memoryCapacityBytes);
    mWorkerId = 0;
    while (mWorkerId == 0) {
      try {
        mMasterClient.connect();
        NetAddress canonicalAddress =
            new NetAddress(mWorkerAddress.getAddress().getCanonicalHostName(),
                mWorkerAddress.getPort());
        mWorkerId =
            mMasterClient.worker_register(canonicalAddress,
                mWorkerSpaceCounter.getCapacityBytes(), 0, new ArrayList<Long>());
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        mWorkerId = 0;
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerId = 0;
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    }

    mLocalDataFolder = new File(dataFolder);
    mLocalUserFolder =
        new File(mLocalDataFolder.toString(), WorkerConf.get().USER_TEMP_RELATIVE_FOLDER);
    mUnderfsWorkerFolder = CommonUtils.concat(COMMON_CONF.UNDERFS_WORKERS_FOLDER, mWorkerId);
    mUnderfsWorkerDataFolder = mUnderfsWorkerFolder + "/data";
    mUnderFs = UnderFileSystem.get(COMMON_CONF.UNDERFS_ADDRESS);
    mUsers = new Users(mLocalUserFolder.toString(), mUnderfsWorkerFolder);

    for (int k = 0; k < WorkerConf.get().WORKER_CHECKPOINT_THREADS; k ++) {
      Thread thread = new Thread(new CheckpointThread(k));
      mCheckpointThreads.add(thread);
      thread.start();
    }

    try {
      initializeWorkerStorage();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    } catch (FileDoesNotExistException e) {
      CommonUtils.runtimeException(e);
    } catch (SuspectedFileSizeException e) {
      CommonUtils.runtimeException(e);
    } catch (BlockInfoException e) {
      CommonUtils.runtimeException(e);
    } catch (TException e) {
      CommonUtils.runtimeException(e);
    }

    LOG.info("Current Worker Info: ID " + mWorkerId + ", ADDRESS: " + mWorkerAddress
        + ", MemoryCapacityBytes: " + mWorkerSpaceCounter.getCapacityBytes());
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId
   *          The id of the block
   */
  public void accessBlock(long blockId) {
    synchronized (mLatestBlockAccessTimeMs) {
      mLatestBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
    }
  }

  private void addBlockId(long blockId, long fileSizeBytes) {
    synchronized (mLatestBlockAccessTimeMs) {
      mLatestBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
      mBlockSizes.put(blockId, fileSizeBytes);
      mMemoryData.add(blockId);
    }
  }

  /**
   * Add the checkpoint information of a file. The information is from the user <code>userId</code>.
   * 
   * @param userId
   *          The user id of the client who send the notification
   * @param fileId
   *          The id of the checkpointed file
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws FailedToCheckpointException
   * @throws BlockInfoException
   * @throws TException
   */
  public void addCheckpoint(long userId, int fileId) throws FileDoesNotExistException,
      SuspectedFileSizeException, FailedToCheckpointException, BlockInfoException, TException {
    // TODO This part need to be changed.
    String srcPath = CommonUtils.concat(getUserUnderfsTempFolder(userId), fileId);
    String dstPath = CommonUtils.concat(COMMON_CONF.UNDERFS_DATA_FOLDER, fileId);
    try {
      if (!mUnderFs.rename(srcPath, dstPath)) {
        throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
      }
    } catch (IOException e) {
      throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
    }
    long fileSize;
    try {
      fileSize = mUnderFs.getFileSize(dstPath);
    } catch (IOException e) {
      throw new FailedToCheckpointException("Failed to getFileSize " + dstPath);
    }
    mMasterClient.addCheckpoint(mWorkerId, fileId, fileSize, dstPath);
  }

  private void addFoundBlock(long blockId, long length) throws FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException, TException {
    addBlockId(blockId, length);
    mMasterClient
        .worker_cacheBlock(mWorkerId, mWorkerSpaceCounter.getUsedBytes(), blockId, length);
  }

  /**
   * Notify the worker to checkpoint the file asynchronously.
   * 
   * @param fileId
   *          The id of the file
   * @return true if succeed, false otherwise
   * @throws IOException
   * @throws TException
   */
  public boolean asyncCheckpoint(int fileId) throws IOException, TException {
    ClientFileInfo fileInfo = mMasterClient.getClientFileInfoById(fileId);

    if (fileInfo.getDependencyId() != -1) {
      synchronized (mDependencyLock) {
        mUncheckpointFiles.add(fileId);
        if (!mDepIdToFiles.containsKey(fileInfo.getDependencyId())) {
          mDepIdToFiles.put(fileInfo.getDependencyId(), new HashSet<Integer>());
        }
        mDepIdToFiles.get(fileInfo.getDependencyId()).add(fileId);
      }
      return true;
    }

    return false;
  }

  /**
   * Notify the worker the block is cached.
   * 
   * @param userId
   *          The user id of the client who send the notification
   * @param blockId
   *          The id of the block
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   * @throws TException
   */
  public void cacheBlock(long userId, long blockId) throws FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException, TException {
    File srcFile = new File(CommonUtils.concat(getUserTempFolder(userId), blockId));
    File dstFile = new File(CommonUtils.concat(mLocalDataFolder, blockId));
    long fileSizeBytes = srcFile.length();
    if (!srcFile.exists()) {
      throw new FileDoesNotExistException("File " + srcFile + " does not exist.");
    }
    if (!srcFile.renameTo(dstFile)) {
      throw new FileDoesNotExistException("Failed to rename file from " + srcFile.getPath()
          + " to " + dstFile.getPath());
    }
    addBlockId(blockId, fileSizeBytes);
    mUsers.addOwnBytes(userId, -fileSizeBytes);
    mMasterClient.worker_cacheBlock(mWorkerId, mWorkerSpaceCounter.getUsedBytes(), blockId,
        fileSizeBytes);
    LOG.info(userId + " " + dstFile);
  }

  /**
   * Check worker's status. This should be executed periodically.
   * <p>
   * It finds the timeout users and cleans them up.
   */
  public void checkStatus() {
    List<Long> removedUsers = mUsers.checkStatus();

    for (long userId : removedUsers) {
      mWorkerSpaceCounter.returnUsedBytes(mUsers.removeUser(userId));
      synchronized (mUsersPerLockedBlock) {
        Set<Long> blockds = mLockedBlocksPerUser.get(userId);
        mLockedBlocksPerUser.remove(userId);
        if (blockds != null) {
          for (long blockId : blockds) {
            try {
              unlockBlock(blockId, userId);
            } catch (TException e) {
              CommonUtils.runtimeException(e);
            }
          }
        }
      }
    }
  }

  /**
   * Remove a block from the memory.
   * 
   * @param blockId
   *          The block to be removed.
   * @return Removed file size in bytes.
   */
  private long freeBlock(long blockId) {
    long freedFileBytes = 0;
    synchronized (mLatestBlockAccessTimeMs) {
      if (mBlockSizes.containsKey(blockId)) {
        mWorkerSpaceCounter.returnUsedBytes(mBlockSizes.get(blockId));
        File srcFile = new File(CommonUtils.concat(mLocalDataFolder, blockId));
        srcFile.delete();
        mLatestBlockAccessTimeMs.remove(blockId);
        freedFileBytes = mBlockSizes.remove(blockId);
        mRemovedBlockList.add(blockId);
        mMemoryData.remove(blockId);
        LOG.info("Removed Data " + blockId);
      } else {
        LOG.warn("File " + blockId + " does not exist in memory.");
      }
    }

    return freedFileBytes;
  }

  /**
   * Remove blocks from the memory.
   * 
   * @param blocks
   *          The list of blocks to be removed.
   */
  public void freeBlocks(List<Long> blocks) {
    for (long blockId : blocks) {
      freeBlock(blockId);
    }
  }

  /**
   * @return The root local data folder of the worker
   * @throws TException
   */
  public String getDataFolder() throws TException {
    return mLocalDataFolder.toString();
  }

  /**
   * @return The orphans' folder in the under file system
   */
  public String getUnderfsOrphansFolder() {
    return mUnderfsOrphansFolder;
  }

  /**
   * Get the local user temporary folder of the specified user.
   * 
   * @param userId
   *          The id of the user
   * @return The local user temporary folder of the specified user
   * @throws TException
   */
  public String getUserTempFolder(long userId) throws TException {
    String ret = mUsers.getUserTempFolder(userId);
    LOG.info("Return UserTempFolder for " + userId + " : " + ret);
    return ret;
  }

  /**
   * Get the user temporary folder in the under file system of the specified user.
   * 
   * @param userId
   *          The id of the user
   * @return The user temporary folder in the under file system
   * @throws TException
   */
  public String getUserUnderfsTempFolder(long userId) throws TException {
    String ret = mUsers.getUserUnderfsTempFolder(userId);
    LOG.info("Return UserHdfsTempFolder for " + userId + " : " + ret);
    return ret;
  }

  /**
   * Heartbeat with the TachyonMaster. Send the removed block list to the Master.
   * 
   * @return The Command received from the Master
   * @throws BlockInfoException
   * @throws TException
   */
  public Command heartbeat() throws BlockInfoException, TException {
    ArrayList<Long> sendRemovedPartitionList = new ArrayList<Long>();
    while (mRemovedBlockList.size() > 0) {
      sendRemovedPartitionList.add(mRemovedBlockList.poll());
    }
    return mMasterClient.worker_heartbeat(mWorkerId, mWorkerSpaceCounter.getUsedBytes(),
        sendRemovedPartitionList);
  }

  private void initializeWorkerStorage() throws IOException, FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException, TException {
    LOG.info("Initializing the worker storage.");
    if (!mLocalDataFolder.exists()) {
      LOG.info("Local folder " + mLocalDataFolder + " does not exist. Creating a new one.");
      mLocalDataFolder.mkdirs();
      mLocalUserFolder.mkdirs();

      CommonUtils.changeLocalFilePermission(mLocalDataFolder.getPath(), "775");
      CommonUtils.changeLocalFilePermission(mLocalUserFolder.getPath(), "775");
      return;
    }

    if (!mLocalDataFolder.isDirectory()) {
      String tmp = "Data folder " + mLocalDataFolder + " is not a folder!";
      LOG.error(tmp);
      throw new IllegalArgumentException(tmp);
    }

    if (mLocalUserFolder.exists()) {
      try {
        FileUtils.deleteDirectory(mLocalUserFolder);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    mLocalUserFolder.mkdir();
    CommonUtils.changeLocalFilePermission(mLocalUserFolder.getPath(), "775");

    mUnderfsOrphansFolder = mUnderfsWorkerFolder + "/orphans";
    if (!mUnderFs.exists(mUnderfsOrphansFolder)) {
      mUnderFs.mkdirs(mUnderfsOrphansFolder, true);
    }

    int cnt = 0;
    for (File tFile : mLocalDataFolder.listFiles()) {
      if (tFile.isFile()) {
        cnt ++;
        LOG.info("File " + cnt + ": " + tFile.getPath() + " with size " + tFile.length() + " Bs.");

        long blockId = CommonUtils.getBlockIdFromFileName(tFile.getName());
        boolean success = mWorkerSpaceCounter.requestSpaceBytes(tFile.length());
        try {
          addFoundBlock(blockId, tFile.length());
        } catch (FileDoesNotExistException e) {
          LOG.error("BlockId: " + blockId + " becomes orphan for: \"" + e.message + "\"");
          LOG.info("Swapout File " + cnt + ": blockId: " + blockId + " to "
              + mUnderfsOrphansFolder);
          swapoutOrphanBlocks(blockId, tFile);
          freeBlock(blockId);
          continue;
        }
        mAddedBlockList.add(blockId);
        if (!success) {
          CommonUtils.runtimeException("Pre-existing files exceed the local memory capacity.");
        }
      }
    }
  }

  /**
   * Lock the block
   * 
   * @param blockId
   *          The id of the block
   * @param userId
   *          The id of the user who locks the block
   * @throws TException
   */
  public void lockBlock(long blockId, long userId) throws TException {
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
   * Use local LRU to evict data, and get <code> requestBytes </code> available space.
   * 
   * @param requestBytes
   *          The data requested.
   * @return <code> true </code> if the space is granted, <code> false </code> if not.
   */
  private boolean memoryEvictionLRU(long requestBytes) {
    Set<Integer> pinList = new HashSet<Integer>();

    try {
      pinList = mMasterClient.worker_getPinIdList();
    } catch (TException e) {
      LOG.error(e.getMessage());
      pinList = new HashSet<Integer>();
    }

    synchronized (mLatestBlockAccessTimeMs) {
      synchronized (mUsersPerLockedBlock) {
        while (mWorkerSpaceCounter.getAvailableBytes() < requestBytes) {
          long blockId = -1;
          long latestTimeMs = Long.MAX_VALUE;
          for (Entry<Long, Long> entry : mLatestBlockAccessTimeMs.entrySet()) {
            if (entry.getValue() < latestTimeMs
                && !pinList.contains(BlockInfo.computeInodeId(entry.getKey()))) {
              if (!mUsersPerLockedBlock.containsKey(entry.getKey())) {
                blockId = entry.getKey();
                latestTimeMs = entry.getValue();
              }
            }
          }
          if (blockId != -1) {
            freeBlock(blockId);
          } else {
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * Register this TachyonWorker to the TachyonMaster
   */
  public void register() {
    long id = 0;
    while (id == 0) {
      try {
        mMasterClient.connect();
        NetAddress canonicalAddress =
            new NetAddress(mWorkerAddress.getAddress().getCanonicalHostName(),
                mWorkerAddress.getPort());
        id =
            mMasterClient.worker_register(canonicalAddress,
                mWorkerSpaceCounter.getCapacityBytes(), 0, new ArrayList<Long>(mMemoryData));
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      }
    }
    mWorkerId = id;
  }

  /**
   * Request space from the worker
   * 
   * @param userId
   *          The id of the user who send the request
   * @param requestBytes
   *          The requested space size, in bytes
   * @return true if succeed, false otherwise
   * @throws TException
   */
  public boolean requestSpace(long userId, long requestBytes) throws TException {
    LOG.info("requestSpace(" + userId + ", " + requestBytes + "): Current available: "
        + mWorkerSpaceCounter.getAvailableBytes() + " requested: " + requestBytes);
    if (mWorkerSpaceCounter.getCapacityBytes() < requestBytes) {
      LOG.info("user_requestSpace(): requested memory size is larger than the total memory on"
          + " the machine.");
      return false;
    }

    while (!mWorkerSpaceCounter.requestSpaceBytes(requestBytes)) {
      if (!memoryEvictionLRU(requestBytes)) {
        return false;
      }
    }

    mUsers.addOwnBytes(userId, requestBytes);

    return true;
  }

  /**
   * Set a new MasterClient and connect to it.
   * 
   * @throws TException
   */
  public void resetMasterClient() throws TException {
    MasterClient tMasterClient = new MasterClient(mMasterAddress);
    tMasterClient.connect();
    mMasterClient = tMasterClient;
  }

  /**
   * Return the space which has been requested
   * 
   * @param userId
   *          The id of the user who wants to return the space
   * @param returnedBytes
   *          The returned space size, in bytes
   * @throws TException
   */
  public void returnSpace(long userId, long returnedBytes) throws TException {
    long preAvailableBytes = mWorkerSpaceCounter.getAvailableBytes();
    if (returnedBytes > mUsers.ownBytes(userId)) {
      LOG.error("User " + userId + " does not own " + returnedBytes + " bytes.");
    } else {
      mWorkerSpaceCounter.returnUsedBytes(returnedBytes);
      mUsers.addOwnBytes(userId, -returnedBytes);
    }

    LOG.info("returnSpace(" + userId + ", " + returnedBytes + ") : " + preAvailableBytes
        + " returned: " + returnedBytes + ". New Available: "
        + mWorkerSpaceCounter.getAvailableBytes());
  }

  /**
   * Disconnect to the Master.
   */
  public void stop() {
    mMasterClient.shutdown();
  }

  /**
   * Swap out those blocks missing INode information onto underFS which can be
   * retrieved by user later. Its cleanup only happens while formating the TFS.
   */
  private void swapoutOrphanBlocks(long blockId, File file) throws IOException {
    RandomAccessFile localFile = new RandomAccessFile(file, "r");
    ByteBuffer buf = localFile.getChannel().map(MapMode.READ_ONLY, 0, file.length());

    String ufsOrphanBlock = CommonUtils.concat(mUnderfsOrphansFolder, blockId);
    OutputStream os = mUnderFs.create(ufsOrphanBlock);
    int BULKSIZE = Constants.KB * 64;
    byte[] bulk = new byte[BULKSIZE];
    for (int k = 0; k < (buf.limit() + BULKSIZE - 1) / BULKSIZE; k ++) {
      int len = BULKSIZE < buf.remaining() ? BULKSIZE : buf.remaining();
      buf.get(bulk, 0, len);
      os.write(bulk, 0, len);
    }
    os.close();

    localFile.close();
  }

  /**
   * Unlock the block
   * 
   * @param blockId
   *          The id of the block
   * @param userId
   *          The id of the user who unlocks the block
   * @throws TException
   */
  public void unlockBlock(long blockId, long userId) throws TException {
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

  /**
   * Handle the user's heartbeat.
   * 
   * @param userId
   *          The id of the user
   * @throws TException
   */
  public void userHeartbeat(long userId) throws TException {
    mUsers.userHeartbeat(userId);
  }
}
