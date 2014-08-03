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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Throwables;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.StorageId;
import tachyon.UnderFileSystem;
import tachyon.Users;
import tachyon.client.BlockHandler;
import tachyon.conf.CommonConf;
import tachyon.conf.WorkerConf;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.Command;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerDirInfo;
import tachyon.util.CommonUtils;
import tachyon.worker.hierarchy.StorageDir;
import tachyon.worker.hierarchy.StorageTier;

/**
 * The structure to store a worker's information in worker node.
 */
public class WorkerStorage {
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
              + mLocalDataFolder + " to " + midPath + " to " + dstPath);

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
                new File(CommonUtils.concat(mLocalDataFolder, fileInfo.blockIds.get(k)));
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
  private final boolean mDropAfterPromote;
  private volatile MasterClient mMasterClient;
  private final InetSocketAddress mMasterAddress;
  private final InetSocketAddress mWorkerAddress;

  private long mWorkerId;
  private final String mLocalDataFolder;
  private final String mLocalUserFolder;
  private final String mUnderfsWorkerFolder;
  private final String mUnderfsWorkerDataFolder;
  private String mUnderfsOrphansFolder;
  private final UnderFileSystem mUnderFs;
  private final Users mUsers;

  // Dependency related lock
  private final Object mDependencyLock = new Object();
  private final Set<Integer> mUncheckpointFiles = new HashSet<Integer>();
  // From dependencyId to files in that set.
  private final Map<Integer, Set<Integer>> mDepIdToFiles = new HashMap<Integer, Set<Integer>>();

  private List<Integer> mPriorityDependencies = new ArrayList<Integer>();

  private final ArrayList<Thread> mCheckpointThreads = new ArrayList<Thread>(
      WorkerConf.get().WORKER_CHECKPOINT_THREADS);

  private StorageTier[] mStorageTiers;
  private long mCapacity;

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
      String dataFolder) {
    COMMON_CONF = CommonConf.get();

    mDropAfterPromote = WorkerConf.get().DROP_AFTER_PROMOTE; // can be set by per tier
    mLocalDataFolder = dataFolder;
    mLocalUserFolder = CommonUtils.concat(dataFolder, WorkerConf.get().USER_TEMP_RELATIVE_FOLDER);

    try {
      initializeWorkerStorage();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    mMasterAddress = masterAddress;
    mMasterClient = new MasterClient(mMasterAddress);

    mWorkerAddress = workerAddress;
    mWorkerId = 0;
    while (mWorkerId == 0) {
      try {
        mMasterClient.connect();
        NetAddress canonicalAddress =
            new NetAddress(mWorkerAddress.getAddress().getCanonicalHostName(),
                mWorkerAddress.getPort());
        mWorkerId =
            mMasterClient.worker_register(canonicalAddress, getCapacity(), 0L,
                new HashMap<Long, List<Long>>());
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
    // initialize ufs after register to the master, avoiding failure in integration test
    mUnderfsWorkerFolder = CommonUtils.concat(COMMON_CONF.UNDERFS_WORKERS_FOLDER, mWorkerId);
    mUnderfsWorkerDataFolder = mUnderfsWorkerFolder + "/data";
    mUnderFs = UnderFileSystem.get(COMMON_CONF.UNDERFS_ADDRESS);
    mUsers = new Users(mLocalUserFolder, mUnderfsWorkerFolder);
    for (int k = 0; k < WorkerConf.get().WORKER_CHECKPOINT_THREADS; k ++) {
      Thread thread = new Thread(new CheckpointThread(k));
      mCheckpointThreads.add(thread);
      thread.start();
    }
    try {
      addFoundBlocks();
    } catch (TException e) {
      throw Throwables.propagate(e);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    LOG.info("Current Worker Info: ID " + mWorkerId + ", ADDRESS: " + mWorkerAddress
        + ", MemoryCapacityBytes: " + getCapacity());
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId
   *          The id of the block
   */
  public void accessBlock(long blockId) {
    StorageDir foundDir = getStorageDirByBlockId(blockId);
    if (foundDir != null) {
      foundDir.accessBlock(blockId);
    }
  }

  /**
   * Add the checkpoint information of a file. The information is from the user <code>userId</code>.
   * 
   * This method is normally triggered from {@link tachyon.client.FileOutStream#close()} if and
   * only if {@link tachyon.client.WriteType#isThrough()} is true. The current implementation
   * of checkpointing is that through {@link tachyon.client.WriteType} operations write to
   * {@link tachyon.UnderFileSystem} on the client's write path, but under a user temp directory
   * (temp directory is defined in the worker as {@link #getUserUnderfsTempFolder(long)}).
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

  /**
   * Report blocks on the worker when initializing worker storage
   * 
   * @throws IOException
   * @throws TException
   */
  private void addFoundBlocks() throws TException, IOException {
    mUnderfsOrphansFolder = mUnderfsWorkerFolder + "/orphans";
    if (!mUnderFs.exists(mUnderfsOrphansFolder)) {
      mUnderFs.mkdirs(mUnderfsOrphansFolder, true);
    }
    for (StorageTier curTier : mStorageTiers) {
      for (StorageDir curDir : curTier.getStorageDirs()) {
        for (Entry<Long, Long> blockSize : curDir.getBlockSizes().entrySet()) {
          try {
            mMasterClient.worker_cacheBlock(mWorkerId, getUsed(), blockSize.getKey(),
                blockSize.getValue(), curDir.getStorageId());
          } catch (FileDoesNotExistException e) {
            LOG.error("BlockId: " + blockSize.getKey() + " Not Exist in Metadata");
            swapoutOrphanBlocks(curDir, blockSize.getKey());
            freeBlock(blockSize.getKey());
          }
        }
      }
    }
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
   * This call is called remotely from {@link tachyon.client.TachyonFS#cacheBlock(long)} which is
   * only ever called from {@link tachyon.client.BlockOutStream#close()} (though its a public api
   * so anyone could call it). There are a few interesting preconditions for this to work.
   * 
   * 1) Client process writes to files locally under a tachyon defined temp directory.
   * 2) Worker process is on the same node as the client
   * 3) Client is talking to the local worker directly
   * 
   * If all conditions are true, then and only then can this method ever be called; all operations
   * work on local files.
   * 
   * @param storageId
   *          The storage id of the dir the block is cached into
   * @param userId
   *          The user id of the client who send the notification
   * @param blockId
   *          The id of the block
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   * @throws TException
   */
  public void cacheBlock(long storageId, long userId, long blockId)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    StorageDir dir = getStorageDirByStorageId(storageId);
    if (dir != null) {
      try {
        dir.cacheBlock(userId, blockId);
      } catch (IOException e) {
        throw new FileDoesNotExistException("cache block failed! storage id:" + storageId);
      }
      long fileSize = dir.getBlockSize(blockId);
      mUsers.addOwnBytes(userId, fileSize);
      mMasterClient.worker_cacheBlock(mWorkerId, getUsed(), blockId, fileSize, storageId);
    } else {
      throw new FileDoesNotExistException("dir not exist! storage id:" + storageId);
    }
  }

  /**
   * Check worker's status. This should be executed periodically.
   * <p>
   * It finds the timeout users and cleans them up.
   * 
   * @throws IOException
   */
  public void checkStatus() throws IOException {
    List<Long> removedUsers = mUsers.checkStatus();
    for (StorageTier curTier : mStorageTiers) {
      for (StorageDir curDir : curTier.getStorageDirs()) {
        curDir.checkStatus(removedUsers);
      }
    }
  }

  /**
   * Remove a block from the memory.
   * 
   * @param blockId
   *          The block to be removed.
   * @return Removed file size in bytes.
   * @throws IOException
   */
  private boolean freeBlock(long blockId) throws IOException {
    StorageDir foundDir = getStorageDirByBlockId(blockId);
    if (foundDir != null) {
      return foundDir.deleteBlock(blockId);
    }
    return false;
  }

  /**
   * Remove blocks from the memory.
   * 
   * This is triggered when the worker heartbeats to the master, which sends a
   * {@link tachyon.thrift.Command} with type {@link tachyon.thrift.CommandType#Free}
   * 
   * @param blocks
   *          The list of blocks to be removed.
   * @throws IOException
   */
  public void freeBlocks(List<Long> blocks) throws IOException {
    for (long blockId : blocks) {
      freeBlock(blockId);
    }
  }

  /**
   * Generate WorkerDirInfo use given StorageDir
   * 
   * @param dir
   *          The dir used to generage WorkerDirInfo
   * @return worker storage info of the storage dir
   */
  public WorkerDirInfo generateWorkerDirInfo(StorageDir dir) throws TachyonException {
    ByteBuffer conf = null;// TODO very tricky... should be CommonUtils.objectToByteBuffer(null)
    if (dir == null) {
      throw new TachyonException("null dir!");
    } else {
      try {
        conf = CommonUtils.objectToByteBuffer(dir.getUfsConf());
      } catch (IOException e) {
        e.printStackTrace();
      }
      return new WorkerDirInfo(dir.getStorageId(), dir.getDirPath(), conf);
    }
  }

  /**
   * Get path of the block's file
   * 
   * @param blockId
   *          The id of the block
   * @return The path of the block's file, if block file doesn't exist, return empty String
   * @throws FileDoesNotExistException
   */
  public String getBlockFilePath(long blockId) throws FileDoesNotExistException {
    StorageDir dir = null;
    dir = getStorageDirByBlockId(blockId);
    if (dir != null) {
      return dir.getBlockFilePath(blockId);
    } else {
      throw new FileDoesNotExistException("block file not found! block id:" + blockId);
    }
  }

  /**
   * Get the file size of given block
   * 
   * @param blockId
   *          The id of the block
   * @return size of the block file
   */
  public long getBlockFileSize(long blockId) throws FileDoesNotExistException, TException {
    StorageDir dir = getStorageDirByBlockId(blockId);
    if (dir == null) {
      throw new FileDoesNotExistException("block not found! block id:" + blockId);
    } else {
      return dir.getBlockSize(blockId);
    }
  }

  /**
   * Get block info on current worker
   */
  public Map<Long, List<Long>> getBlockInfos() {
    Map<Long, List<Long>> blockInfos = new HashMap<Long, List<Long>>();
    for (StorageTier curTier : mStorageTiers) {
      for (StorageDir curDir : curTier.getStorageDirs()) {
        Set<Long> blockSet = curDir.getBlockIds();
        blockInfos.put(curDir.getStorageId(), new ArrayList<Long>(blockSet));
      }
    }
    return blockInfos;
  }

  /**
   * Get the capacity of the worker storage
   */
  public long getCapacity() {
    return mCapacity;
  }

  /**
   * @return The local data folder of the worker
   * @throws TException
   */
  public String getDataFolder() {
    return mLocalDataFolder;
  }

  /**
   * Get dir info that contains given block id
   * 
   * @param blockId
   *          The id of the block
   * @return info of the dir containing the block's file
   */
  public WorkerDirInfo getDirInfoByBlockId(long blockId) throws TachyonException {
    StorageDir foundDir = getStorageDirByBlockId(blockId);
    return generateWorkerDirInfo(foundDir);
  }

  /**
   * Get dir info that contains given block id
   * 
   * @param storageId
   *          The storage id of the dir
   * @return info of the dir with the storage id
   */
  public WorkerDirInfo getDirInfoByStorageId(long storageId) throws TachyonException {
    StorageDir foundDir = getStorageDirByBlockId(storageId);
    return generateWorkerDirInfo(foundDir);
  }

  /**
   * Get storage dir that contains given block id
   * 
   * @param blockId
   *          The id of the block
   * @return storage dir containing the block's file, null if block file doesn't exist on current
   *         worker
   */
  public StorageDir getStorageDirByBlockId(long blockId) {
    StorageDir foundDir = null;
    for (StorageTier curTier : mStorageTiers) {
      foundDir = curTier.getStorageDirByBlockId(blockId);
      if (foundDir != null) {
        break;
      } else {
        curTier = curTier.getNextStorageTier();
      }
    }
    return foundDir;
  }

  /**
   * Get storage dir from the given dir id
   * 
   * @param storageId
   *          The storage id of the dir
   * @return storage dir of the storage id
   */
  public StorageDir getStorageDirByStorageId(long storageId) {
    int tierIndex = StorageId.getStorageTierIndex(storageId);
    int dirIndex = StorageId.getStorageDirIndex(storageId);
    if (tierIndex < mStorageTiers.length) {
      return mStorageTiers[tierIndex].getStorageDirByIndex(dirIndex);
    } else {
      LOG.error("level index excceed boundary! levelIndex:" + tierIndex);
      return null;
    }
  }

  /**
   * @return The orphans' folder in the under file system
   */
  public String getUnderfsOrphansFolder() {
    return mUnderfsOrphansFolder;
  }

  /**
   * Get current used space of the worker storage
   */
  public long getUsed() {
    long used = 0;
    for (StorageTier curTier : mStorageTiers) {
      used += curTier.getUsed();
    }
    return used;
  }

  /**
   * Get the local user temporary folder of the specified user.
   * 
   * This method is a wrapper around {@link tachyon.Users#getUserTempFolder(long)}, and as
   * such should be referentially transparent with {@link tachyon.Users#getUserTempFolder(long)}.
   * In the context of {@code this}, this call will output the result of path concat of
   * {@link #mLocalUserFolder} with the provided {@literal userId}.
   * 
   * This method differs from {@link #getUserUnderfsTempFolder(long)} in the context of where write
   * operations end up. This temp folder generated lives inside the tachyon file system, and as
   * such, will be stored in memory.
   * 
   * @see tachyon.Users#getUserTempFolder(long)
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
   * This method is a wrapper around {@link tachyon.Users#getUserUnderfsTempFolder(long)}, and as
   * such should be referentially transparent with {@link Users#getUserUnderfsTempFolder(long)}. In
   * the context of {@code this}, this call will output the result of path concat of
   * {@link #mUnderfsWorkerFolder} with the provided {@literal userId}.
   * 
   * This method differs from {@link #getUserTempFolder(long)} in the context of where write
   * operations end up. This temp folder generated lives inside the {@link tachyon.UnderFileSystem},
   * and as such, will be stored remotely, most likely on disk.
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
   * @throws IOException
   */
  public Command heartbeat() throws BlockInfoException, TException, IOException {
    Set<Long> removedBlockIdSet = new HashSet<Long>();
    Map<Long, List<Long>> swappedBlocks = new HashMap<Long, List<Long>>();
    List<Long> removedBlockIds = new ArrayList<Long>();
    for (StorageTier curTier : mStorageTiers) {
      removedBlockIdSet.addAll(curTier.getRemovedBlockList());
    }
    for (StorageTier curTier : mStorageTiers) {  // TODO optimize this part
      for (StorageDir curDir : curTier.getStorageDirs()) {
        List<Long> blockIds = new ArrayList<Long>();
        for (long blockId : removedBlockIdSet) {
          if (curDir.containsBlock(blockId)) {
            blockIds.add(blockId);
          }
        }
        if (blockIds.size() > 0) {
          swappedBlocks.put(curDir.getStorageId(), blockIds);
          removedBlockIdSet.removeAll(blockIds);
        }
      }
    }
    removedBlockIds.addAll(removedBlockIdSet);
    return mMasterClient.worker_heartbeat(mWorkerId, getUsed(), removedBlockIds, swappedBlocks);
  }

  /**
   * initialize worker storage
   * 
   * @throws IOException
   */
  public void initializeWorkerStorage() throws IOException {
    mStorageTiers = new StorageTier[WorkerConf.get().MAX_HIERARCHY_STORAGE_LEVEL];
    for (int level = 0; level < mStorageTiers.length; level ++) {
      String[] dirPaths = WorkerConf.get().STORAGE_LEVEL_DIRS[level].split(",");
      for (int i = 0; i < dirPaths.length; i ++) {
        dirPaths[i] = dirPaths[i].trim();
      }
      String Alias = WorkerConf.get().STORAGE_LEVEL_ALIAS[level];
      String[] strDirCapacities = WorkerConf.get().STORAGE_LEVEL_DIR_QUOTA[level].split(",");
      long[] dirCapacities = new long[dirPaths.length];
      for (int i = 0, j = 0; i < dirPaths.length; i ++) {
        // The storage directory quota for each storage directory
        dirCapacities[i] = CommonUtils.parseSpaceSize(strDirCapacities[j].trim());
        if (j < strDirCapacities.length - 1) {
          j ++;
        }
      }
      StorageTier curTier =
          new StorageTier(level, Alias, dirPaths, dirCapacities, mLocalDataFolder,
              mLocalUserFolder, null); // TODO add conf for UFS
      curTier.initialize();
      mCapacity += curTier.getCapacity();
      mStorageTiers[level] = curTier;
      if (level > 0) {
        mStorageTiers[level - 1].setNextStorageTier(curTier);
      }
    }
  }

  /**
   * Lock the block
   * 
   * Used internally to make sure blocks are unmodified, but also used in
   * {@link tachyon.client.TachyonFS} for cacheing blocks locally for users. When a user tries
   * to read a block ({@link tachyon.client.TachyonFile#readByteBuffer()}), the client will attempt
   * to cache the block on the local users's node, while the user is reading from the local block,
   * the given block is locked and unlocked once read.
   * 
   * @param blockId
   *          The id of the block
   * @param userId
   *          The id of the user who locks the block
   */
  public void lockBlock(long blockId, long userId) {
    StorageDir foundDir = null;
    foundDir = getStorageDirByBlockId(blockId);
    if (foundDir != null) {
      foundDir.lockBlock(blockId, userId);
    }
  }

  /**
   * promote the block into top storage tier
   * 
   * @param userId
   * @param blockId
   *          The id of the block
   * @param readTypeValue
   *          Read type the block
   */
  public boolean promoteBlock(long userId, long blockId) throws TException {
    StorageDir srcDir = getStorageDirByBlockId(blockId);
    long storageId = srcDir.getStorageId();
    if (StorageId.getStorageLevelAliasValue(storageId) != mStorageTiers[0].getStorageLevelAlias()
        .getValue()) {
      long blockSize = srcDir.getBlockSize(blockId);
      lockBlock(userId, blockId);
      StorageDir dstDir = requestSpace(userId, blockSize);
      unlockBlock(userId, blockId);
      if (dstDir == null) {
        LOG.error("promote block failed! blockId:" + blockId);
        return false;
      }
      boolean result;
      try {
        if (mDropAfterPromote) {
          result = srcDir.moveBlock(blockId, dstDir);
        } else {
          result = srcDir.copyBlock(blockId, dstDir);
        }
      } catch (IOException e) {
        LOG.error("Error during promote block! blockId:" + blockId);
        return false;
      }
      mMasterClient.worker_cacheBlock(mWorkerId, getCapacity(), blockId, blockSize,
          dstDir.getStorageId());
      return result;
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
        id = mMasterClient.worker_register(canonicalAddress, getCapacity(), 0L, getBlockInfos());
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
   * @return storage dir assigned if succeed, null otherwise
   */
  public StorageDir requestSpace(long userId, long requestBytes) throws TachyonException {
    Set<Integer> pinList;

    try {
      pinList = mMasterClient.worker_getPinIdList();
    } catch (TException e) {
      LOG.error(e.getMessage());
      pinList = new HashSet<Integer>();
    }

    StorageDir dir = null;
    try {
      dir = mStorageTiers[0].requestSpace(userId, requestBytes, pinList);
    } catch (IOException e) {
      throw new TachyonException(e.getMessage());
    }

    if (dir != null) {
      mUsers.addOwnBytes(userId, requestBytes);
    }
    return dir;
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
   * @param storageId
   *          The storage id of the dir that return the space to
   * @param userId
   *          The id of the user who wants to return the space
   * @param returnedBytes
   *          The returned space size, in bytes
   */
  public void returnSpace(long storageId, long userId, long returnedBytes) {
    StorageDir dir = getStorageDirByStorageId(storageId);
    if (returnedBytes > mUsers.ownBytes(userId)) {
      LOG.error("User " + userId + " does not own " + returnedBytes + " bytes.");
    } else {
      dir.returnSpace(userId, returnedBytes);
      mUsers.addOwnBytes(userId, -returnedBytes);
    }

    LOG.info("returnSpace(" + userId + ", " + returnedBytes + ") : " + " New Available: "
        + dir.getAvailable());
  }

  /**
   * Stop connection with Master
   */
  public void stop() {
    mMasterClient.shutdown();
  }

  /**
   * Swap out those blocks missing INode information onto underFS which can be
   * retrieved by user later. Its cleanup only happens while formating the
   * TFS.
   * 
   * @param dir
   * @param blockId
   * @throws IOException
   */
  private void swapoutOrphanBlocks(StorageDir dir, long blockId) throws IOException {
    BlockHandler bhSrc = dir.getBlockHandler(blockId);
    long fileLen = dir.getBlockSize(blockId);
    ByteBuffer buf = bhSrc.readByteBuffer(0, (int) fileLen);
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
  }

  /**
   * Unlock the block
   * 
   * Used internally to make sure blocks are unmodified, but also used in
   * {@link tachyon.client.TachyonFS} for cacheing blocks locally for users. When a user tries
   * to read a block ({@link tachyon.client.TachyonFile#readByteBuffer()}), the client will attempt
   * to cache the block on the local users's node, while the user is reading from the local block,
   * the given block is locked and unlocked once read.
   * 
   * @param blockId
   *          The id of the block
   * @param userId
   *          The id of the user who unlocks the block
   */
  public void unlockBlock(long blockId, long userId) {
    StorageDir foundDir = null;
    foundDir = getStorageDirByBlockId(blockId);
    if (foundDir != null) {
      foundDir.unlockBlock(blockId, userId);
    }
  }

  /**
   * Handle the user's heartbeat.
   * 
   * @param userId
   *          The id of the user
   */
  public void userHeartbeat(long userId) {
    mUsers.userHeartbeat(userId);
  }
}
