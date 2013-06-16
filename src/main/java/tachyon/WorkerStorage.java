package tachyon;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
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

import tachyon.conf.CommonConf;
import tachyon.conf.WorkerConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.Command;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * The structure to store a worker's information in worker node.
 */
public class WorkerStorage {
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

  private BlockingQueue<Long> mRemovedBlockList = 
      new ArrayBlockingQueue<Long>(Constants.WORKER_FILES_QUEUE_SIZE);
  private BlockingQueue<Long> mAddedBlockList = 
      new ArrayBlockingQueue<Long>(Constants.WORKER_FILES_QUEUE_SIZE);

  private File mLocalDataFolder;
  private File mLocalUserFolder;
  private String mUnderfsWorkerFolder;
  private UnderFileSystem mUnderFs;

  private Users mUsers;

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
        mWorkerId = mMasterClient.worker_register(
            new NetAddress(mWorkerAddress.getHostName(), mWorkerAddress.getPort()),
            mWorkerSpaceCounter.getCapacityBytes(), 0, new ArrayList<Long>());
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        mWorkerId = 0;
        CommonUtils.sleepMs(LOG, 1000);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerId = 0;
        CommonUtils.sleepMs(LOG, 1000);
      }
    }

    mLocalDataFolder = new File(dataFolder);
    mLocalUserFolder =
        new File(mLocalDataFolder.toString(), WorkerConf.get().USER_TEMP_RELATIVE_FOLDER);
    mUnderfsWorkerFolder = COMMON_CONF.UNDERFS_WORKERS_FOLDER + "/" + mWorkerId;
    mUnderFs = UnderFileSystem.get(COMMON_CONF.UNDERFS_ADDRESS);
    mUsers = new Users(mLocalUserFolder.toString(), mUnderfsWorkerFolder);

    try {
      initializeWorkerStorage();
    } catch (FileDoesNotExistException e) {
      CommonUtils.runtimeException(e);
    } catch (SuspectedFileSizeException e) {
      CommonUtils.runtimeException(e);
    } catch (BlockInfoException e) {
      CommonUtils.runtimeException(e);
    } catch (TException e) {
      CommonUtils.runtimeException(e);
    } 

    LOG.info("Current Worker Info: ID " + mWorkerId + ", ADDRESS: " + mWorkerAddress +
        ", MemoryCapacityBytes: " + mWorkerSpaceCounter.getCapacityBytes());
  }

  public void accessBlock(long blockId) {
    synchronized (mLatestBlockAccessTimeMs) {
      mLatestBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
    }
  }

  public void addCheckpoint(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, 
      FailedToCheckpointException, BlockInfoException, TException {
    // TODO This part need to be changed.
    String srcPath = getUserUnderfsTempFolder(userId) + "/" + fileId;
    String dstPath = COMMON_CONF.UNDERFS_DATA_FOLDER + "/" + fileId;
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

  private void addFoundBlock(long blockId, long length)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    addBlockId(blockId, length);
    mMasterClient.worker_cacheBlock(mWorkerId, mWorkerSpaceCounter.getUsedBytes(), blockId, length);
  }

  private void addBlockId(long blockId, long fileSizeBytes) {
    synchronized (mLatestBlockAccessTimeMs) {
      mLatestBlockAccessTimeMs.put(blockId, System.currentTimeMillis());
      mBlockSizes.put(blockId, fileSizeBytes);
      mMemoryData.add(blockId);
    }
  }

  public void cacheBlock(long userId, long blockId)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    File srcFile = new File(getUserTempFolder(userId) + "/" + blockId);
    File dstFile = new File(mLocalDataFolder + "/" + blockId);
    long fileSizeBytes = srcFile.length(); 
    if (!srcFile.exists()) {
      throw new FileDoesNotExistException("File " + srcFile + " does not exist.");
    }
    if (!srcFile.renameTo(dstFile)) {
      throw new FileDoesNotExistException("Failed to rename file from " + srcFile.getPath() +
          " to " + dstFile.getPath());
    }
    addBlockId(blockId, fileSizeBytes);
    mUsers.addOwnBytes(userId, - fileSizeBytes);
    mMasterClient.worker_cacheBlock(mWorkerId, 
        mWorkerSpaceCounter.getUsedBytes(), blockId, fileSizeBytes);
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
   * @param blockId The block to be removed.
   * @return Removed file size in bytes.
   */
  private synchronized long freeBlock(long blockId) {
    Long freedFileBytes = null;
    if (mBlockSizes.containsKey(blockId)) {
      mWorkerSpaceCounter.returnUsedBytes(mBlockSizes.get(blockId));
      File srcFile = new File(mLocalDataFolder + "/" + blockId);
      srcFile.delete();
      synchronized (mLatestBlockAccessTimeMs) {
        mLatestBlockAccessTimeMs.remove(blockId);
        freedFileBytes = mBlockSizes.remove(blockId);
        mRemovedBlockList.add(blockId);
        mMemoryData.remove(blockId);
      }
      LOG.info("Removed Data " + blockId);
    } else {
      LOG.warn("File " + blockId + " does not exist in memory.");
    }

    return freedFileBytes == null ? 0 : freedFileBytes;
  }

  /**
   * Remove blocks from the memory.
   * @param blocks The list of blocks to be removed.
   */
  public void freeBlocks(List<Long> blocks) {
    for (long blockId: blocks) {
      freeBlock(blockId);
    }
  }

  public String getDataFolder() throws TException {
    return mLocalDataFolder.toString();
  }

  public String getUserTempFolder(long userId) throws TException {
    String ret = mUsers.getUserTempFolder(userId);
    LOG.info("Return UserTempFolder for " + userId + " : " + ret);
    return ret;
  }

  public String getUserUnderfsTempFolder(long userId) throws TException {
    String ret = mUsers.getUserUnderfsTempFolder(userId);
    LOG.info("Return UserHdfsTempFolder for " + userId + " : " + ret);
    return ret;
  }

  public Command heartbeat() throws BlockInfoException, TException {
    ArrayList<Long> sendRemovedPartitionList = new ArrayList<Long>();
    while (mRemovedBlockList.size() > 0) {
      sendRemovedPartitionList.add(mRemovedBlockList.poll());
    }
    return mMasterClient.worker_heartbeat(mWorkerId, mWorkerSpaceCounter.getUsedBytes(),
        sendRemovedPartitionList);
  }

  private void initializeWorkerStorage() 
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    LOG.info("Initializing the worker storage.");
    if (!mLocalDataFolder.exists()) {
      LOG.info("Local folder " + mLocalDataFolder + " does not exist. Creating a new one.");
      mLocalDataFolder.mkdir();
      mLocalUserFolder.mkdir();
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

    int cnt = 0;
    for (File tFile : mLocalDataFolder.listFiles()) {
      if (tFile.isFile()) {
        cnt ++;
        LOG.info("File " + cnt + ": " + tFile.getPath() + " with size " + tFile.length() + " Bs.");

        long blockId = CommonUtils.getBlockIdFromFileName(tFile.getName());
        boolean success = mWorkerSpaceCounter.requestSpaceBytes(tFile.length());
        addFoundBlock(blockId, tFile.length());
        mAddedBlockList.add(blockId);
        if (!success) {
          CommonUtils.runtimeException("Pre-existing files exceed the local memory capacity.");
        }
      }
    }
  }

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
   * @param requestBytes The data requested.
   * @return <code> true </code> if the space is granteed, <code> false </code> if not.
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
          long fileId = -1;
          long latestTimeMs = Long.MAX_VALUE;
          for (Entry<Long, Long> entry : mLatestBlockAccessTimeMs.entrySet()) {
            if (entry.getValue() < latestTimeMs && !pinList.contains(entry.getKey())) {
              if(!mUsersPerLockedBlock.containsKey(entry.getKey())) {
                fileId = entry.getKey();
                latestTimeMs = entry.getValue();
              }
            }
          }
          if (fileId != -1) {
            freeBlock(fileId);
          } else {
            return false;
          }
        }
      }
    }

    return true;
  }

  public void register() {
    long id = 0;
    while (id == 0) {
      try {
        mMasterClient.connect();
        id = mMasterClient.worker_register(
            new NetAddress(mWorkerAddress.getHostName(), mWorkerAddress.getPort()),
            mWorkerSpaceCounter.getCapacityBytes(), 0, new ArrayList<Long>(mMemoryData));
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleepMs(LOG, 1000);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleepMs(LOG, 1000);
      }
    }
    mWorkerId = id;
  }

  public void returnSpace(long userId, long returnedBytes) throws TException {
    long preAvailableBytes = mWorkerSpaceCounter.getAvailableBytes();
    if (returnedBytes > mUsers.ownBytes(userId)) {
      LOG.error("User " + userId + " does not own " + returnedBytes + " bytes.");
    } else {
      mWorkerSpaceCounter.returnUsedBytes(returnedBytes);
      mUsers.addOwnBytes(userId, - returnedBytes);
    }

    LOG.info("returnSpace(" + userId + ", " + returnedBytes + ") : " +
        preAvailableBytes + " returned: " + returnedBytes + ". New Available: " +
        mWorkerSpaceCounter.getAvailableBytes());
  }

  public boolean requestSpace(long userId, long requestBytes) throws TException {
    LOG.info("requestSpace(" + userId + ", " + requestBytes + "): Current available: " +
        mWorkerSpaceCounter.getAvailableBytes() + " requested: " + requestBytes);
    if (mWorkerSpaceCounter.getCapacityBytes() < requestBytes) {
      LOG.info("user_requestSpace(): requested memory size is larger than the total memory on" +
          " the machine.");
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

  public void resetMasterClient() {
    MasterClient tMasterClient = new MasterClient(mMasterAddress);
    tMasterClient.connect();
    mMasterClient = tMasterClient;
  }

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

  public void userHeartbeat(long userId) throws TException {
    mUsers.userHeartbeat(userId);
  }
}