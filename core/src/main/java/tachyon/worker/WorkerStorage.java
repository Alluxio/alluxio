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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.UnderFileSystem;
import tachyon.Users;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.Command;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.OutOfSpaceException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.util.CommonUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.hierarchy.StorageDir;
import tachyon.worker.hierarchy.StorageTier;

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
            CommonUtils.sleepMs(LOG, Constants.SECOND_MS, true);
            continue;
          }

          // TODO checkpoint process. In future, move from midPath to dstPath should be done by
          // master
          String midPath = CommonUtils.concat(mUfsWorkerDataFolder, fileId);
          String ufsDataFolder = mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER, "/tachyon/data");
          String dstPath = CommonUtils.concat(ufsDataFolder, fileId);
          LOG.info("Thread " + mId + " is checkpointing file " + fileId + " from " + mDataFolder
              + " to " + midPath + " to " + dstPath);

          if (mCheckpointUfs == null) {
            mCheckpointUfs = UnderFileSystem.get(midPath, mTachyonConf);
          }

          final long startCopyTimeMs = System.currentTimeMillis();
          ClientFileInfo fileInfo = mMasterClient.getFileStatus(fileId, "");
          if (!fileInfo.isComplete) {
            LOG.error("File {} is not complete!", fileInfo);
            continue;
          }

          StorageDir[] storageDirs = new StorageDir[fileInfo.blockIds.size()];
          OutputStream os = null;
          long fileSizeByte = 0;
          try {
            for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
              long blockId = fileInfo.blockIds.get(k);
              storageDirs[k] = lockBlock(blockId, Users.CHECKPOINT_USER_ID);
              if (storageDirs[k] == null) {
                throw new IOException("Block doesn't exist! blockId:" + blockId);
              }
            }
            os = mCheckpointUfs.create(midPath, (int) fileInfo.getBlockSizeByte());
            for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
              ByteBuffer byteBuffer = storageDirs[k].getBlockData(fileInfo.blockIds.get(k), 0, -1);
              byte[] buf = new byte[16 * Constants.KB];
              int writeLen;
              while (byteBuffer.remaining() > 0) {
                if (byteBuffer.remaining() >= buf.length) {
                  writeLen = buf.length;
                } else {
                  writeLen = byteBuffer.remaining();
                }
                byteBuffer.get(buf, 0, writeLen);
                os.write(buf, 0, writeLen);
              }
              CommonUtils.cleanDirectBuffer(byteBuffer);
            }
          } finally {
            for (int k = 0; k < fileInfo.blockIds.size(); k ++) {
              if (storageDirs[k] != null) {
                storageDirs[k].unlockBlock(fileInfo.blockIds.get(k), Users.CHECKPOINT_USER_ID);
              }
            }
            if (os != null) {
              os.close();
            }
          }
          if (!mCheckpointUfs.rename(midPath, dstPath)) {
            LOG.error("Failed to rename from " + midPath + " to " + dstPath);
          }
          mMasterClient.addCheckpoint(mWorkerId, fileId, fileSizeByte, dstPath);
          int capMbSec =
              mTachyonConf.getInt(Constants.WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC,
                  Constants.SECOND_MS);
          long shouldTakeMs = (long) (1000.0 * fileSizeByte / Constants.MB / capMbSec);
          long currentTimeMs = System.currentTimeMillis();
          if (startCopyTimeMs + shouldTakeMs > currentTimeMs) {
            long shouldSleepMs = startCopyTimeMs + shouldTakeMs - currentTimeMs;
            LOG.info("Checkpointed last file " + fileId + " took "
                + (currentTimeMs - startCopyTimeMs) + " ms. Need to sleep " + shouldSleepMs
                + " ms.");
            CommonUtils.sleepMs(LOG, shouldSleepMs, true);
          }
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private volatile MasterClient mMasterClient;
  private final InetSocketAddress mMasterAddress;
  private NetAddress mWorkerAddress;

  private long mWorkerId;

  private final String mDataFolder;
  private final String mUserFolder;
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

  private final ExecutorService mCheckpointExecutor;

  private final ExecutorService mExecutorService;
  private long mCapacityBytes;
  private ArrayList<StorageTier> mStorageTiers;
  private final BlockingQueue<Long> mRemovedBlockIdList = new ArrayBlockingQueue<Long>(
      Constants.WORKER_BLOCKS_QUEUE_SIZE);
  /** Mapping from temporary block Information to StorageDir in which the block is */
  private final Map<Pair<Long, Long>, StorageDir> mTempBlockLocation = Collections
      .synchronizedMap(new HashMap<Pair<Long, Long>, StorageDir>());
  /** Mapping from user id to id list of temporary blocks which are being written by the user */
  private final Multimap<Long, Long> mUserIdToTempBlockIds = Multimaps
      .synchronizedMultimap(HashMultimap.<Long, Long>create());

  private final TachyonConf mTachyonConf;

  /**
   * Main logic behind the worker process.
   *
   * This object is lazily initialized. Before an object of this call should be used,
   * {@link #initialize} must be called.
   *
   * @param masterAddress The TachyonMaster's address
   * @param executorService
   * @param tachyonConf The instance of TachyonConf to be used. If null the instantiate new one.
   */
  public WorkerStorage(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    mExecutorService = executorService;
    mTachyonConf = tachyonConf;
    mMasterAddress = masterAddress;
    mMasterClient = new MasterClient(mMasterAddress, mExecutorService, mTachyonConf);

    mDataFolder = mTachyonConf.get(Constants.WORKER_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);

    String userTmpFolder = mTachyonConf.get(Constants.WORKER_USER_TEMP_RELATIVE_FOLDER, "users");
    mUserFolder = CommonUtils.concat(mDataFolder, userTmpFolder);

    int checkpointThreads = mTachyonConf.getInt(Constants.WORKER_CHECKPOINT_THREADS, 1);
    mCheckpointExecutor =
        Executors.newFixedThreadPool(checkpointThreads, ThreadFactoryUtils.build("checkpoint-%d"));
  }

  public void initialize(final NetAddress address) {
    mWorkerAddress = address;

    try {
      initializeStorageTier();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    register();

    String tachyonHome = mTachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
    String ufsAddress = mTachyonConf.get(Constants.UNDERFS_ADDRESS, tachyonHome + "/underfs");
    String ufsWorkerFolder =
        mTachyonConf.get(Constants.UNDERFS_WORKERS_FOLDER, ufsAddress + "/tachyon/workers");
    mUfsWorkerFolder = CommonUtils.concat(ufsWorkerFolder, mWorkerId);
    mUfsWorkerDataFolder = mUfsWorkerFolder + "/data";
    mUfs = UnderFileSystem.get(ufsAddress, mTachyonConf);
    mUsers = new Users(mUfsWorkerFolder, mTachyonConf);

    int checkpointThreads = mTachyonConf.getInt(Constants.WORKER_CHECKPOINT_THREADS, 1);
    for (int k = 0; k < checkpointThreads; k ++) {
      mCheckpointExecutor.submit(new CheckpointThread(k));
    }

    try {
      addFoundBlocks();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } catch (BlockInfoException e) {
      throw Throwables.propagate(e);
    }

    LOG.info("Current Worker ID: {}, mWorkerAddress: {}, CapacityBytes: {}", mWorkerId,
        mWorkerAddress, mCapacityBytes);
  }

  /**
   * Update the latest block access time on the worker.
   *
   * @param blockId The id of the block
   */
  void accessBlock(long blockId) {
    StorageDir foundDir = getStorageDirByBlockId(blockId);
    if (foundDir != null) {
      foundDir.accessBlock(blockId);
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
    String ufsDataFolder = mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER, "/tachyon/data");
    String dstPath = CommonUtils.concat(ufsDataFolder, fileId);
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

  /**
   * Report blocks on the worker when initializing worker storage
   *
   * @throws IOException
   * @throws BlockInfoException
   */
  private void addFoundBlocks() throws IOException, BlockInfoException {
    mUfsOrphansFolder = mUfsWorkerFolder + "/orphans";
    if (!mUfs.exists(mUfsOrphansFolder)) {
      mUfs.mkdirs(mUfsOrphansFolder, true);
    }
    for (StorageTier curStorageTier : mStorageTiers) {
      for (StorageDir curStorageDir : curStorageTier.getStorageDirs()) {
        for (Entry<Long, Long> blockSize : curStorageDir.getBlockSizes()) {
          try {
            mMasterClient.worker_cacheBlock(mWorkerId,
                getUsedBytesOnTiers().get(curStorageTier.getAlias().getValue() - 1),
                curStorageDir.getStorageDirId(), blockSize.getKey(), blockSize.getValue());
          } catch (FileDoesNotExistException e) {
            LOG.error("Block not exist in metadata! blockId:{}", blockSize.getKey());
            swapoutOrphanBlocks(curStorageDir, blockSize.getKey());
            freeBlock(blockSize.getKey());
          }
        }
      }
    }
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
   * only ever called from {@link tachyon.client.BlockOutStream#close()} (though its a public api so
   * anyone could call it). There are a few interesting preconditions for this to work.
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
   * @throws BlockInfoException
   * @throws IOException
   */
  public void cacheBlock(long userId, long blockId) throws FileDoesNotExistException,
      BlockInfoException, IOException {
    StorageDir storageDir = mTempBlockLocation.remove(new Pair<Long, Long>(userId, blockId));
    if (storageDir == null) {
      throw new FileDoesNotExistException("Block doesn't exist! blockId:" + blockId);
    }
    mUserIdToTempBlockIds.remove(userId, blockId);
    boolean result = false;
    try {
      result = storageDir.cacheBlock(userId, blockId);
    } catch (IOException e) {
      throw new FileDoesNotExistException("Failed to cache block! blockId:" + blockId);
    }
    if (result) {
      long blockSize = storageDir.getBlockSize(blockId);
      mMasterClient.worker_cacheBlock(
          mWorkerId,
          getUsedBytesOnTiers().get(
              StorageDirId.getStorageLevelAliasValue(storageDir.getStorageDirId()) - 1),
          storageDir.getStorageDirId(), blockId, blockSize);
    }
  }

  /**
   * Cancel the block which is being written by some user
   *
   * @param userId The id of the user who wants to cancel the block
   * @param blockId The id of the block that is cancelled
   */
  public void cancelBlock(long userId, long blockId) {
    StorageDir storageDir = mTempBlockLocation.remove(new Pair<Long, Long>(userId, blockId));

    if (storageDir != null) {
      mUserIdToTempBlockIds.remove(userId, blockId);
      try {
        storageDir.cancelBlock(userId, blockId);
      } catch (IOException e) {
        LOG.error("Failed to cancel block! blockId:{}", blockId);
      }
    }
  }

  /**
   * Check worker's status. This should be executed periodically.
   * <p>
   * It finds the timeout users and cleans them up.
   */
  public void checkStatus() {
    List<Long> removedUsers = mUsers.checkStatus();

    for (long userId : removedUsers) {
      Collection<Long> tempBlockIdList = mUserIdToTempBlockIds.removeAll(userId);
      for (Long blockId : tempBlockIdList) {
        mTempBlockLocation.remove(new Pair<Long, Long>(userId, blockId));
      }
      for (StorageTier storageTier : mStorageTiers) {
        for (StorageDir storageDir : storageTier.getStorageDirs()) {
          storageDir.cleanUserResources(userId, tempBlockIdList);
        }
      }
      mUsers.removeUser(userId);
    }
  }

  /**
   * Remove a block from Tachyon cache space.
   *
   * @param blockId The block to be removed.
   */
  private void freeBlock(long blockId) {
    for (StorageTier storageTier : mStorageTiers) {
      for (StorageDir storageDir : storageTier.getStorageDirs()) {
        if (storageDir.containsBlock(blockId)) {
          try {
            storageDir.deleteBlock(blockId);
          } catch (IOException e) {
            LOG.error("Failed to delete block file! blockId:{}", blockId);
          }
        }
      }
    }
    mRemovedBlockIdList.add(blockId);
  }

  /**
   * Remove blocks from Tachyon cache space.
   *
   * This is triggered when the worker heartbeats to the master, which sends a
   * {@link tachyon.thrift.Command} with type {@link tachyon.thrift.CommandType#Free}
   *
   * @param blockIds The id list of blocks to be removed.
   */
  public void freeBlocks(List<Long> blockIds) {
    for (long blockId : blockIds) {
      freeBlock(blockId);
    }
  }

  /**
   * Get StorageDir which contains specified block
   *
   * @param blockId the id of the block
   * @return StorageDir which contains the block
   */
  public StorageDir getStorageDirByBlockId(long blockId) {
    StorageDir storageDir = null;
    for (StorageTier storageTier : mStorageTiers) {
      storageDir = storageTier.getStorageDirByBlockId(blockId);
      if (storageDir != null) {
        return storageDir;
      }
    }
    return null;
  }

  /**
   * @return The orphans' folder in the under file system
   */
  public String getUfsOrphansFolder() {
    return mUfsOrphansFolder;
  }

  /**
   * Get used bytes on each storage tier
   *
   * @return used bytes on each storage tier
   */
  private List<Long> getUsedBytesOnTiers() {
    List<Long> usedBytes = new ArrayList<Long>(Collections.nCopies(StorageLevelAlias.SIZE, 0L));
    for (StorageTier curTier : mStorageTiers) {
      int tier = curTier.getAlias().getValue() - 1;
      usedBytes.set(tier, usedBytes.get(tier) + curTier.getUsedBytes());
    }
    return usedBytes;
  }

  /**
   * Get the user temporary folder in the under file system of the specified user.
   *
   * This method is a wrapper around {@link tachyon.Users#getUserUfsTempFolder(long)}, and as such
   * should be referentially transparent with {@link Users#getUserUfsTempFolder(long)}. In the
   * context of {@code this}, this call will output the result of path concat of
   * {@link #mUfsWorkerFolder} with the provided {@literal userId}.
   *
   * This temp folder generated lives inside the {@link tachyon.UnderFileSystem}, and as such, will
   * be stored remotely, most likely on disk.
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
   * Heartbeat with the TachyonMaster. Send the removed block list and added block list to the
   * Master.
   *
   * @return The Command received from the Master
   * @throws IOException
   */
  public Command heartbeat() throws IOException {
    List<Long> removedBlockIds = new ArrayList<Long>();
    Map<Long, List<Long>> addedBlockIds = new HashMap<Long, List<Long>>();

    mRemovedBlockIdList.drainTo(removedBlockIds);

    for (StorageTier storageTier : mStorageTiers) {
      for (StorageDir storageDir : storageTier.getStorageDirs()) {
        addedBlockIds.put(storageDir.getStorageDirId(), storageDir.getAddedBlockIdList());
      }
    }
    return mMasterClient.worker_heartbeat(mWorkerId, getUsedBytesOnTiers(), removedBlockIds,
        addedBlockIds);
  }

  /**
   * Initialize StorageTiers on current WorkerStorage
   *
   * @throws IOException
   */
  public void initializeStorageTier() throws IOException {
    int maxStorageLevels = mTachyonConf.getInt(Constants.WORKER_MAX_HIERARCHY_STORAGE_LEVEL, 1);

    mStorageTiers = new ArrayList<StorageTier>(maxStorageLevels);
    for (int k = 0; k < maxStorageLevels; k ++) {
      mStorageTiers.add(null);
    }
    StorageTier nextStorageTier = null;
    for (int level = maxStorageLevels - 1; level >= 0; level --) {
      String tierLevelAliasProp = "tachyon.worker.hierarchystore.level" + level + ".alias";
      String tierLevelDirPath = "tachyon.worker.hierarchystore.level" + level + ".dirs.path";
      String tierDirsQuotaProp = "tachyon.worker.hierarchystore.level" + level + ".dirs.quota";
      int index = level;
      if (index >= Constants.DEFAULT_STORAGE_TIER_DIR_QUOTA.length) {
        index = level - 1;
      }

      StorageLevelAlias storageLevelAlias =
          mTachyonConf.getEnum(tierLevelAliasProp, StorageLevelAlias.MEM);

      String[] dirPaths = mTachyonConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
      for (int i = 0; i < dirPaths.length; i ++) {
        dirPaths[i] = dirPaths[i].trim();
      }

      String tierDirsQuota =
          mTachyonConf.get(tierDirsQuotaProp, Constants.DEFAULT_STORAGE_TIER_DIR_QUOTA[index]);

      for (int i = 0; i < dirPaths.length; i ++) {
        dirPaths[i] = dirPaths[i].trim();
      }
      String[] dirCapacityStrings = tierDirsQuota.split(",");
      long[] dirCapacities = new long[dirPaths.length];
      for (int i = 0, j = 0; i < dirPaths.length; i ++) {
        // The storage directory quota for each storage directory
        dirCapacities[i] = CommonUtils.parseSpaceSize(dirCapacityStrings[j].trim());
        if (j < dirCapacityStrings.length - 1) {
          j ++;
        }
      }
      StorageTier curTier =
          new StorageTier(level, storageLevelAlias, dirPaths, dirCapacities, mDataFolder,
              mUserFolder, nextStorageTier, null, mTachyonConf); // TODO add conf for UFS
      curTier.initialize();
      mCapacityBytes += curTier.getCapacityBytes();
      mStorageTiers.set(level, curTier);
      nextStorageTier = curTier;
    }
  }

  /**
   * Lock the block by some user
   *
   * Used internally to make sure blocks are unmodified, but also used in
   * {@link tachyon.client.TachyonFS} for caching blocks locally for users. When a user tries to
   * read a block ({@link tachyon.client.TachyonFile#readByteBuffer(int)} ()}), the client will
   * attempt to cache the block on the local users's node, while the user is reading from the local
   * block, the given block is locked and unlocked once read.
   *
   * @param blockId The id of the block
   * @param userId The id of the user who locks the block
   * @return the StorageDir in which the block is locked
   */
  public StorageDir lockBlock(long blockId, long userId) {
    for (StorageTier tier : mStorageTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.lockBlock(blockId, userId)) {
          return dir;
        }
      }
    }
    LOG.warn("Failed to lock block! blockId:{}", blockId);
    return null;
  }

  /**
   * If the block is not on top StorageTier, promote block to top StorageTier
   *
   * @param blockId the id of the block
   * @return true if success, false otherwise
   */
  public boolean promoteBlock(long blockId) {
    final long userId = Users.MIGRATE_DATA_USER_ID;
    StorageDir storageDir = lockBlock(blockId, userId);
    if (storageDir == null) {
      return false;
    } else if (StorageDirId.getStorageLevelAliasValue(storageDir.getStorageDirId()) != mStorageTiers
        .get(0).getAlias().getValue()) {
      long blockSize = storageDir.getBlockSize(blockId);
      StorageDir dstStorageDir = requestSpace(null, userId, blockSize);
      if (dstStorageDir == null) {
        LOG.error("Failed to promote block! blockId:{}", blockId);
        storageDir.unlockBlock(blockId, userId);
        return false;
      }
      boolean result = false;
      try {
        try {
          result = storageDir.copyBlock(blockId, dstStorageDir);
        } finally {
          storageDir.unlockBlock(blockId, userId);
        }
        if (result) {
          storageDir.deleteBlock(blockId);
        }
        return result;
      } catch (IOException e) {
        LOG.error("Failed to promote block! blockId:{}", blockId);
        return false;
      }
    } else {
      unlockBlock(blockId, userId);
      return true;
    }
  }

  /**
   * Register this TachyonWorker to the TachyonMaster
   */
  public void register() {
    long id = 0;
    Map<Long, List<Long>> blockIdLists = new HashMap<Long, List<Long>>();
    List<Long> capacityBytesOnTiers =
        new ArrayList<Long>(Collections.nCopies(StorageLevelAlias.SIZE, 0L));
    for (StorageTier curStorageTier : mStorageTiers) {
      int tier = curStorageTier.getAlias().getValue() - 1;
      capacityBytesOnTiers.set(tier,
          capacityBytesOnTiers.get(tier) + curStorageTier.getCapacityBytes());
      for (StorageDir curStorageDir : curStorageTier.getStorageDirs()) {
        Set<Long> blockSet = curStorageDir.getBlockIds();
        blockIdLists.put(curStorageDir.getStorageDirId(), new ArrayList<Long>(blockSet));
      }
    }
    while (id == 0) {
      try {
        id =
            mMasterClient.worker_register(mWorkerAddress, capacityBytesOnTiers,
                getUsedBytesOnTiers(), blockIdLists);
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
   * Get temporary file path for some block, it is used to choose appropriate StorageDir for some
   * block file with specified initial size.
   *
   * @param userId the id of the user who wants to write the file
   * @param blockId the id of the block
   * @param initialBytes the initial size allocated for the block
   * @return the temporary path of the block file
   * @throws OutOfSpaceException
   * @throws FileAlreadyExistException
   */
  public String requestBlockLocation(long userId, long blockId, long initialBytes)
      throws OutOfSpaceException, FileAlreadyExistException {
    if (mTempBlockLocation.containsKey(new Pair<Long, Long>(userId, blockId))) {
      throw new FileAlreadyExistException(String.format("Block file is being written! userId(%d)"
          + " blockId(%d)", userId, blockId));
    }

    StorageDir storageDir = requestSpace(null, userId, initialBytes);
    if (storageDir == null) {
      throw new OutOfSpaceException(String.format("Failed to allocate space for block! blockId(%d)"
          + " sizeBytes(%d)", blockId, initialBytes));
    }
    mTempBlockLocation.put(new Pair<Long, Long>(userId, blockId), storageDir);
    mUserIdToTempBlockIds.put(userId, blockId);
    storageDir.updateTempBlockAllocatedBytes(userId, blockId, initialBytes);

    return storageDir.getUserTempFilePath(userId, blockId);
  }

  /**
   * Request space from the worker, and expecting worker return the appropriate StorageDir which has
   * enough space for the requested space size
   *
   * @param dirCandidate The StorageDir in which the space will be allocated.
   * @param userId The id of the user who send the request
   * @param requestBytes The requested space size, in bytes
   * @return StorageDir assigned, null if failed
   */
  private StorageDir requestSpace(StorageDir dirCandidate, long userId, long requestBytes) {
    Set<Integer> pinList;

    try {
      pinList = mMasterClient.worker_getPinIdList();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      pinList = new HashSet<Integer>();
    }

    StorageDir dir = null;
    List<Long> removedBlockIds = new ArrayList<Long>();
    try {
      if (dirCandidate == null) {
        // if StorageDir candidate is not set, request space from all available StorageDirs
        dir = mStorageTiers.get(0).requestSpace(userId, requestBytes, pinList, removedBlockIds);
      } else { // request space from the StorageDir specified
        if (mStorageTiers.get(0).requestSpace(dirCandidate, userId, requestBytes, pinList,
            removedBlockIds)) {
          dir = dirCandidate;
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    } finally {
      if (removedBlockIds.size() > 0) {
        mRemovedBlockIdList.addAll(removedBlockIds);
      }
    }

    return dir;
  }

  /**
   * Request space from the specified StorageDir, it is used for requesting space for the block
   * which is partially written in some StorageDir
   *
   * @param userId The id of the user who send the request
   * @param blockId The id of the block that the space is allocated for
   * @param requestBytes The requested space size, in bytes
   * @return true if succeed, false otherwise
   * @throws FileDoesNotExistException
   */
  public boolean requestSpace(long userId, long blockId, long requestBytes)
      throws FileDoesNotExistException {
    StorageDir storageDir = mTempBlockLocation.get(new Pair<Long, Long>(userId, blockId));
    if (storageDir == null) {
      throw new FileDoesNotExistException("Temporary block file doesn't exist! blockId:" + blockId);
    }

    if (storageDir == requestSpace(storageDir, userId, requestBytes)) {
      storageDir.updateTempBlockAllocatedBytes(userId, blockId, requestBytes);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Set a new MasterClient and connect to it.
   */
  public void resetMasterClient() {
    MasterClient tMasterClient = new MasterClient(mMasterAddress, mExecutorService, mTachyonConf);
    mMasterClient = tMasterClient;
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
  private void swapoutOrphanBlocks(StorageDir storageDir, long blockId) throws IOException {
    ByteBuffer buf = storageDir.getBlockData(blockId, 0, -1);
    String ufsOrphanBlock = CommonUtils.concat(mUfsOrphansFolder, blockId);
    OutputStream os = mUfs.create(ufsOrphanBlock);
    final int bulkSize = Constants.KB * 64;
    byte[] bulk = new byte[bulkSize];
    try {
      for (int k = 0; k < (buf.limit() + bulkSize - 1) / bulkSize; k ++) {
        int len = bulkSize < buf.remaining() ? bulkSize : buf.remaining();
        buf.get(bulk, 0, len);
        os.write(bulk, 0, len);
      }
    } finally {
      os.close();
      CommonUtils.cleanDirectBuffer(buf);
    }
  }

  /**
   * Unlock the block
   * 
   * Used internally to make sure blocks are unmodified, but also used in
   * {@link tachyon.client.TachyonFS} for caching blocks locally for users. When a user tries to
   * read a block ({@link tachyon.client.TachyonFile#readByteBuffer(int)}), the client will attempt
   * to cache the block on the local users's node, while the user is reading from the local block,
   * the given block is locked and unlocked once read.
   * 
   * @param blockId The id of the block
   * @param userId The id of the user who unlocks the block
   * @return true if success, false otherwise
   */
  public boolean unlockBlock(long blockId, long userId) {
    for (StorageTier tier : mStorageTiers) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.unlockBlock(blockId, userId)) {
          return true;
        }
      }
    }
    LOG.warn("Failed to unlock block! blockId:{}", blockId);
    return false;
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
