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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.io.Closer;

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
import tachyon.util.ThreadFactoryUtils;

/**
 * The structure to store a worker's information in worker node.
 */
public class WorkerStorage {
  /**
   * The CheckpointThread, used to checkpoint the files belong to the worker.
   */
  public class CheckpointThread implements Runnable {
    private final int mId;
    private UnderFileSystem mCheckpointUfs = null;

    public CheckpointThread(int id) {
      mId = id;
    }

    // This method assumes the mDependencyLock has been acquired.
    private int getFileIdBasedOnPriorityDependency() {
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
    private int getFileIdFromOneDependency(int depId) {
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
    private int getRandomUncheckpointedFile() {
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

    private List<Integer> getSortedPriorityDependencyList() throws IOException {
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
      while (!Thread.currentThread().isInterrupted()) {
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
            LOG.debug("Thread {} has nothing to checkpoint. Sleep for 1 sec.", mId);
            CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
            continue;
          }

          // TODO checkpoint process. In future, move from midPath to dstPath should be done by
          // master
          String midPath = CommonUtils.concat(mUfsWorkerDataFolder, fileId);
          String dstPath = CommonUtils.concat(CommonConf.get().UNDERFS_DATA_FOLDER, fileId);
          LOG.info("Thread " + mId + " is checkpointing file " + fileId + " from "
              + mLocalDataFolder.toString() + " to " + midPath + " to " + dstPath);

          if (mCheckpointUfs == null) {
            mCheckpointUfs = UnderFileSystem.get(midPath);
          }

          final long startCopyTimeMs = System.currentTimeMillis();
          ClientFileInfo fileInfo = mMasterClient.getFileStatus(fileId, "");
          if (!fileInfo.isComplete) {
            LOG.error("File " + fileInfo + " is not complete!");
            continue;
          }
          for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
            lockBlock(fileInfo.blockIds.get(k), Users.CHECKPOINT_USER_ID);
          }
          Closer closer = Closer.create();
          long fileSizeByte = 0;
          try {
            OutputStream os = 
                closer.register(mCheckpointUfs.create(midPath, (int) fileInfo.getBlockSizeByte()));
            for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
              File tempFile =
                  new File(CommonUtils.concat(mLocalDataFolder.toString(),
                      fileInfo.blockIds.get(k)));
              fileSizeByte += tempFile.length();
              InputStream is = closer.register(new FileInputStream(tempFile));
              byte[] buf = new byte[16 * Constants.KB];
              int got = is.read(buf);
              while (got != -1) {
                os.write(buf, 0, got);
                got = is.read(buf);
              }
            }
          } finally {
            closer.close();
            for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
              unlockBlock(fileInfo.blockIds.get(k), Users.CHECKPOINT_USER_ID);
            }
          }
          if (!mCheckpointUfs.rename(midPath, dstPath)) {
            LOG.error("Failed to rename from " + midPath + " to " + dstPath);
          }
          mMasterClient.addCheckpoint(mWorkerId, fileId, fileSizeByte, dstPath);
          for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
            unlockBlock(fileInfo.blockIds.get(k), Users.CHECKPOINT_USER_ID);
          }

          int cap = WorkerConf.get().WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC;
          long shouldTakeMs =
              (long) (1000.0 * fileSizeByte / Constants.MB / cap);
          long currentTimeMs = System.currentTimeMillis();
          if (startCopyTimeMs + shouldTakeMs > currentTimeMs) {
            long shouldSleepMs = startCopyTimeMs + shouldTakeMs - currentTimeMs;
            LOG.info("Checkpointed last file " + fileId + " took "
                + (currentTimeMs - startCopyTimeMs) + " ms. Need to sleep " + shouldSleepMs
                + " ms.");
            CommonUtils.sleepMs(LOG, shouldSleepMs);
          }
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final CommonConf mCommonConf;
  private volatile MasterClient mMasterClient;
  private final InetSocketAddress mMasterAddress;
  private NetAddress mWorkerAddress;
  private final SpaceCounter mSpaceCounter;

  private long mWorkerId;
  private final Set<Long> mMemoryData = new HashSet<Long>();
  private final Map<Long, Long> mBlockSizes = new HashMap<Long, Long>();

  private final Map<Long, Long> mBlockIdToLatestAccessTimeMs = new HashMap<Long, Long>();
  private final Map<Long, Set<Long>> mLockedBlockIdToUserId = new HashMap<Long, Set<Long>>();

  private final Map<Long, Set<Long>> mLockedBlocksPerUser = new HashMap<Long, Set<Long>>();
  private final BlockingQueue<Long> mRemovedBlockList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);

  private final BlockingQueue<Long> mAddedBlockList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  private final File mLocalDataFolder;
  private final File mLocalUserFolder;
  private String mUfsWorkerFolder;
  private String mUfsWorkerDataFolder;
  private String mUfsOrphansFolder;

  private UnderFileSystem mUfs;

  private Users mUsers;
  // Dependency related lock
  private final Object mDependencyLock = new Object();
  private final Set<Integer> mUncheckpointFiles = new HashSet<Integer>();
  // From dependencyId to files in that set.
  private final Map<Integer, Set<Integer>> mDepIdToFiles = new HashMap<Integer, Set<Integer>>();

  private List<Integer> mPriorityDependencies = new ArrayList<Integer>();

  private final ExecutorService mCheckpointExecutor = Executors.newFixedThreadPool(
      WorkerConf.get().WORKER_CHECKPOINT_THREADS,
      ThreadFactoryUtils.build("checkpoint-%d"));

  private final ExecutorService mExecutorService;

  /**
   * Main logic behind the worker process.
   * 
   * This object is lazily initialized. Before an object of this call should be used,
   * {@link #initialize} must be called.
   * 
   * @param masterAddress The TachyonMaster's address
   * @param dataFolder This TachyonWorker's local folder's path
   * @param memoryCapacityBytes The maximum memory space this TachyonWorker can use, in bytes
   * @param executorService
   */
  public WorkerStorage(InetSocketAddress masterAddress, String dataFolder,
      long memoryCapacityBytes, ExecutorService executorService) {
    mExecutorService = executorService;
    mCommonConf = CommonConf.get();

    mMasterAddress = masterAddress;
    mMasterClient = new MasterClient(mMasterAddress, mExecutorService);
    mLocalDataFolder = new File(dataFolder);

    mSpaceCounter = new SpaceCounter(memoryCapacityBytes);
    mLocalUserFolder = new File(mLocalDataFolder, WorkerConf.USER_TEMP_RELATIVE_FOLDER);
  }

  public void initialize(final NetAddress address) {
    mWorkerAddress = address;

    register();

    mUfsWorkerFolder = CommonUtils.concat(mCommonConf.UNDERFS_WORKERS_FOLDER, mWorkerId);
    mUfsWorkerDataFolder = mUfsWorkerFolder + "/data";
    mUfs = UnderFileSystem.get(mCommonConf.UNDERFS_ADDRESS);
    mUsers = new Users(mLocalUserFolder.toString(), mUfsWorkerFolder);

    for (int k = 0; k < WorkerConf.get().WORKER_CHECKPOINT_THREADS; k ++) {
      mCheckpointExecutor.submit(new CheckpointThread(k));
    }

    try {
      initializeWorkerStorage();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } catch (FileDoesNotExistException e) {
      throw Throwables.propagate(e);
    } catch (SuspectedFileSizeException e) {
      throw Throwables.propagate(e);
    } catch (BlockInfoException e) {
      throw Throwables.propagate(e);
    }

    LOG.info("Current Worker Info: ID " + mWorkerId + ", mWorkerAddress: " + mWorkerAddress
        + ", MemoryCapacityBytes: " + mSpaceCounter.getCapacityBytes());
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId The id of the block
   */
  void accessBlock(long blockId) {
    synchronized (mBlockIdToLatestAccessTimeMs) {
      mBlockIdToLatestAccessTimeMs.put(blockId, System.currentTimeMillis());
    }
  }

  private void addBlockId(long blockId, long fileSizeBytes) {
    synchronized (mBlockIdToLatestAccessTimeMs) {
      mBlockIdToLatestAccessTimeMs.put(blockId, System.currentTimeMillis());
      mBlockSizes.put(blockId, fileSizeBytes);
      mMemoryData.add(blockId);
    }
  }

  /**
   * Add the checkpoint information of a file. The information is from the user <code>userId</code>.
   * 
   * This method is normally triggered from {@link tachyon.client.FileOutStream#close()} if and only
   * if {@link tachyon.client.WriteType#isThrough()} is true. The current implementation of
   * checkpointing is that through {@link tachyon.client.WriteType} operations write to
   * {@link tachyon.UnderFileSystem} on the client's write path, but under a user temp directory
   * (temp directory is defined in the worker as {@link #getUserUfsTempFolder(long)}).
   * 
   * @param userId The user id of the client who send the notification
   * @param fileId The id of the checkpointed file
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws FailedToCheckpointException
   * @throws BlockInfoException
   */
  public void addCheckpoint(long userId, int fileId) throws FileDoesNotExistException,
      SuspectedFileSizeException, FailedToCheckpointException, BlockInfoException, IOException {
    // TODO This part need to be changed.
    String srcPath = CommonUtils.concat(getUserUfsTempFolder(userId), fileId);
    String dstPath = CommonUtils.concat(mCommonConf.UNDERFS_DATA_FOLDER, fileId);
    try {
      if (!mUfs.rename(srcPath, dstPath)) {
        throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
      }
    } catch (IOException e) {
      throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
    }
    long fileSize;
    try {
      fileSize = mUfs.getFileSize(dstPath);
    } catch (IOException e) {
      throw new FailedToCheckpointException("Failed to getFileSize " + dstPath);
    }
    mMasterClient.addCheckpoint(mWorkerId, fileId, fileSize, dstPath);
  }

  private void addFoundBlock(long blockId, long length) throws FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException, IOException {
    addBlockId(blockId, length);
    mMasterClient.worker_cacheBlock(mWorkerId, mSpaceCounter.getUsedBytes(), blockId, length);
  }

  /**
   * Notify the worker to checkpoint the file asynchronously.
   * 
   * @param fileId The id of the file
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public boolean asyncCheckpoint(int fileId) throws IOException {
    ClientFileInfo fileInfo = mMasterClient.getFileStatus(fileId, "");

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
   * only ever called from {@link tachyon.client.WritableBlockChannel#close()} (though its a public
   * api so anyone could call it). There are a few interesting preconditions for this to work.
   * 
   * 1) Client process writes to files locally under a tachyon defined temp directory. 2) Worker
   * process is on the same node as the client 3) Client is talking to the local worker directly
   * 
   * If all conditions are true, then and only then can this method ever be called; all operations
   * work on local files.
   * 
   * @param userId The user id of the client who send the notification
   * @param blockId The id of the block
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   * @throws IOException
   */
  public void cacheBlock(long userId, long blockId) throws FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException, IOException {
    File srcFile = new File(CommonUtils.concat(getUserLocalTempFolder(userId), blockId));
    File dstFile = new File(CommonUtils.concat(mLocalDataFolder, blockId));
    long fileSizeBytes = srcFile.length();
    if (!srcFile.exists()) {
      throw new FileDoesNotExistException("File " + srcFile + " does not exist.");
    }
    synchronized (mBlockIdToLatestAccessTimeMs) {
      if (!srcFile.renameTo(dstFile)) {
        throw new FileDoesNotExistException("Failed to rename file from " + srcFile.getPath()
            + " to " + dstFile.getPath());
      }
      if (mBlockSizes.containsKey(blockId)) {
        mSpaceCounter.returnUsedBytes(mBlockSizes.get(blockId));
      }
      addBlockId(blockId, fileSizeBytes);
      mUsers.addOwnBytes(userId, -fileSizeBytes);
      mMasterClient.worker_cacheBlock(mWorkerId, mSpaceCounter.getUsedBytes(), blockId,
          fileSizeBytes);
    }
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
      mSpaceCounter.returnUsedBytes(mUsers.removeUser(userId));
      synchronized (mLockedBlockIdToUserId) {
        Set<Long> blockds = mLockedBlocksPerUser.get(userId);
        mLockedBlocksPerUser.remove(userId);
        if (blockds != null) {
          for (long blockId : blockds) {
            unlockBlock(blockId, userId);
          }
        }
      }
    }
  }

  /**
   * Remove a block from the memory.
   * 
   * @param blockId The block to be removed.
   * @return Removed file size in bytes.
   */
  private long freeBlock(long blockId) {
    long freedFileBytes = 0;
    synchronized (mBlockIdToLatestAccessTimeMs) {
      if (mBlockSizes.containsKey(blockId)) {
        mSpaceCounter.returnUsedBytes(mBlockSizes.get(blockId));
        File srcFile = new File(CommonUtils.concat(mLocalDataFolder, blockId));
        srcFile.delete();
        mBlockIdToLatestAccessTimeMs.remove(blockId);
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
   * This is triggered when the worker heartbeats to the master, which sends a
   * {@link tachyon.thrift.Command} with type {@link tachyon.thrift.CommandType#Free}
   * 
   * @param blocks The list of blocks to be removed.
   */
  public void freeBlocks(List<Long> blocks) {
    for (long blockId : blocks) {
      freeBlock(blockId);
    }
  }

  /**
   * @return The root local data folder of the worker
   */
  public String getDataFolder() {
    return mLocalDataFolder.toString();
  }

  /**
   * @return The orphans' folder in the under file system
   */
  public String getUfsOrphansFolder() {
    return mUfsOrphansFolder;
  }

  /**
   * Get the local user temporary folder of the specified user.
   * 
   * This method is a wrapper around {@link tachyon.Users#getUserTempFolder(long)}, and as such
   * should be referentially transparent with {@link tachyon.Users#getUserTempFolder(long)}. In the
   * context of {@code this}, this call will output the result of path concat of
   * {@link #mLocalUserFolder} with the provided {@literal userId}.
   * 
   * This method differs from {@link #getUserUfsTempFolder(long)} in the context of where write
   * operations end up. This temp folder generated lives inside the tachyon file system, and as
   * such, will be stored in memory.
   * 
   * @see tachyon.Users#getUserTempFolder(long)
   * 
   * @param userId The id of the user
   * @return The local user temporary folder of the specified user
   */
  public String getUserLocalTempFolder(long userId) {
    String ret = mUsers.getUserTempFolder(userId);
    LOG.info("Return UserTempFolder for " + userId + " : " + ret);
    return ret;
  }

  /**
   * Get the user temporary folder in the under file system of the specified user.
   * 
   * This method is a wrapper around {@link tachyon.Users#getUserUfsTempFolder(long)}, and as such
   * should be referentially transparent with {@link Users#getUserUfsTempFolder(long)}. In the
   * context of {@code this}, this call will output the result of path concat of
   * {@link #mUfsWorkerFolder} with the provided {@literal userId}.
   * 
   * This method differs from {@link #getUserLocalTempFolder(long)} in the context of where write
   * operations end up. This temp folder generated lives inside the {@link tachyon.UnderFileSystem},
   * and as such, will be stored remotely, most likely on disk.
   * 
   * @param userId The id of the user
   * @return The user temporary folder in the under file system
   */
  public String getUserUfsTempFolder(long userId) {
    String ret = mUsers.getUserUfsTempFolder(userId);
    LOG.info("Return UserHdfsTempFolder for " + userId + " : " + ret);
    return ret;
  }

  /**
   * Heartbeat with the TachyonMaster. Send the removed block list to the Master.
   * 
   * @return The Command received from the Master
   * @throws IOException
   */
  public Command heartbeat() throws IOException {
    ArrayList<Long> sendRemovedPartitionList = new ArrayList<Long>();
    while (mRemovedBlockList.size() > 0) {
      sendRemovedPartitionList.add(mRemovedBlockList.poll());
    }
    return mMasterClient.worker_heartbeat(mWorkerId, mSpaceCounter.getUsedBytes(),
        sendRemovedPartitionList);
  }

  private void initializeWorkerStorage() throws IOException, FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException {
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

    mUfsOrphansFolder = mUfsWorkerFolder + "/orphans";
    if (!mUfs.exists(mUfsOrphansFolder)) {
      mUfs.mkdirs(mUfsOrphansFolder, true);
    }

    int cnt = 0;
    for (File tFile : mLocalDataFolder.listFiles()) {
      if (tFile.isFile()) {
        cnt ++;
        LOG.info("File " + cnt + ": " + tFile.getPath() + " with size " + tFile.length() + " Bs.");

        long blockId = CommonUtils.getBlockIdFromFileName(tFile.getName());
        boolean success = mSpaceCounter.requestSpaceBytes(tFile.length());
        try {
          addFoundBlock(blockId, tFile.length());
        } catch (FileDoesNotExistException e) {
          LOG.error("BlockId: " + blockId + " becomes orphan for: \"" + e.message + "\"");
          LOG.info("Swapout File " + cnt + ": blockId: " + blockId + " to " + mUfsOrphansFolder);
          swapoutOrphanBlocks(blockId, tFile);
          freeBlock(blockId);
          continue;
        }
        mAddedBlockList.add(blockId);
        if (!success) {
          throw new RuntimeException("Pre-existing files exceed the local memory capacity.");
        }
      }
    }
  }

  /**
   * Lock the block
   * 
   * Used internally to make sure blocks are unmodified, but also used in
   * {@link tachyon.client.TachyonFS} for caching blocks locally for users. When a user tries to
   * read a block ({@link tachyon.client.TachyonFile#readByteBuffer(int)} ()}), the client will
   * attempt to cache the block on the local users's node, while the user is reading from the local
   * block, the given block is locked and unlocked once read.
   * 
   * @param blockId The id of the block
   * @param userId The id of the user who locks the block
   */
  public void lockBlock(long blockId, long userId) {
    synchronized (mLockedBlockIdToUserId) {
      if (!mLockedBlockIdToUserId.containsKey(blockId)) {
        mLockedBlockIdToUserId.put(blockId, new HashSet<Long>());
      }
      mLockedBlockIdToUserId.get(blockId).add(userId);

      if (!mLockedBlocksPerUser.containsKey(userId)) {
        mLockedBlocksPerUser.put(userId, new HashSet<Long>());
      }
      mLockedBlocksPerUser.get(userId).add(blockId);
    }
  }

  /**
   * Use local LRU to evict data, and get <code> requestBytes </code> available space.
   * 
   * @param requestBytes The data requested.
   * @return <code> true </code> if the space is granted, <code> false </code> if not.
   */
  private boolean memoryEvictionLRU(long requestBytes) {
    Set<Integer> pinList;

    try {
      pinList = mMasterClient.worker_getPinIdList();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      pinList = new HashSet<Integer>();
    }

    synchronized (mBlockIdToLatestAccessTimeMs) {
      synchronized (mLockedBlockIdToUserId) {
        while (mSpaceCounter.getAvailableBytes() < requestBytes) {
          long blockId = -1;
          long latestTimeMs = Long.MAX_VALUE;
          for (Entry<Long, Long> entry : mBlockIdToLatestAccessTimeMs.entrySet()) {
            if (entry.getValue() < latestTimeMs
                && !pinList.contains(BlockInfo.computeInodeId(entry.getKey()))) {
              if (!mLockedBlockIdToUserId.containsKey(entry.getKey())) {
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
        id =
            mMasterClient.worker_register(mWorkerAddress, mSpaceCounter.getCapacityBytes(),
                mSpaceCounter.getUsedBytes(), new ArrayList<Long>(mMemoryData));
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
      } catch (IOException e) {
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
   * @param userId The id of the user who send the request
   * @param requestBytes The requested space size, in bytes
   * @return true if succeed, false otherwise
   */
  public boolean requestSpace(long userId, long requestBytes) {
    LOG.info("requestSpace(" + userId + ", " + requestBytes + "): Current available: "
        + mSpaceCounter.getAvailableBytes() + " requested: " + requestBytes);
    if (mSpaceCounter.getCapacityBytes() < requestBytes) {
      LOG.info("user_requestSpace(): requested memory size is larger than the total memory on"
          + " the machine.");
      return false;
    }

    while (!mSpaceCounter.requestSpaceBytes(requestBytes)) {
      if (!memoryEvictionLRU(requestBytes)) {
        return false;
      }
    }

    mUsers.addOwnBytes(userId, requestBytes);

    return true;
  }

  /**
   * Set a new MasterClient and connect to it.
   */
  public void resetMasterClient() {
    MasterClient tMasterClient = new MasterClient(mMasterAddress, mExecutorService);
    mMasterClient = tMasterClient;
  }

  /**
   * Return the space which has been requested
   * 
   * @param userId The id of the user who wants to return the space
   * @param returnedBytes The returned space size, in bytes
   */
  public void returnSpace(long userId, long returnedBytes) {
    long preAvailableBytes = mSpaceCounter.getAvailableBytes();
    if (returnedBytes > mUsers.ownBytes(userId)) {
      LOG.error("User " + userId + " does not own " + returnedBytes + " bytes.");
    } else {
      mSpaceCounter.returnUsedBytes(returnedBytes);
      mUsers.addOwnBytes(userId, -returnedBytes);
    }

    LOG.info("returnSpace(" + userId + ", " + returnedBytes + ") : " + preAvailableBytes
        + " returned: " + returnedBytes + ". New Available: " + mSpaceCounter.getAvailableBytes());
  }

  /**
   * Disconnect to the Master.
   */
  public void stop() {
    mMasterClient.shutdown();
    // this will make sure that we don't move on till checkpoint threads are cleaned up
    // needed or tests can get resource issues
    mCheckpointExecutor.shutdownNow();
    try {
      mCheckpointExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // didn't stop in time, this is a bug!
      throw Throwables.propagate(e);
    }
  }

  /**
   * Swap out those blocks missing INode information onto underFS which can be retrieved by user
   * later. Its cleanup only happens while formating the mTachyonFS.
   */
  private void swapoutOrphanBlocks(long blockId, File file) throws IOException {
    RandomAccessFile localFile = new RandomAccessFile(file, "r");
    ByteBuffer buf = localFile.getChannel().map(MapMode.READ_ONLY, 0, file.length());

    String ufsOrphanBlock = CommonUtils.concat(mUfsOrphansFolder, blockId);
    OutputStream os = mUfs.create(ufsOrphanBlock);
    final int bulkSize = Constants.KB * 64;
    byte[] bulk = new byte[bulkSize];
    for (int k = 0; k < (buf.limit() + bulkSize - 1) / bulkSize; k ++) {
      int len = bulkSize < buf.remaining() ? bulkSize : buf.remaining();
      buf.get(bulk, 0, len);
      os.write(bulk, 0, len);
    }
    os.close();

    localFile.close();
  }

  /**
   * Unlock the block
   * 
   * Used internally to make sure blocks are unmodified, but also used in
   * {@link tachyon.client.TachyonFS} for cacheing blocks locally for users. When a user tries to
   * read a block ({@link tachyon.client.TachyonFile#readByteBuffer(int)}), the client will attempt
   * to cache the block on the local users's node, while the user is reading from the local block,
   * the given block is locked and unlocked once read.
   * 
   * @param blockId The id of the block
   * @param userId The id of the user who unlocks the block
   */
  public void unlockBlock(long blockId, long userId) {
    synchronized (mLockedBlockIdToUserId) {
      if (mLockedBlockIdToUserId.containsKey(blockId)) {
        mLockedBlockIdToUserId.get(blockId).remove(userId);
        if (mLockedBlockIdToUserId.get(blockId).size() == 0) {
          mLockedBlockIdToUserId.remove(blockId);
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
   * @param userId The id of the user
   */
  public void userHeartbeat(long userId) {
    mUsers.userHeartbeat(userId);
  }
}
