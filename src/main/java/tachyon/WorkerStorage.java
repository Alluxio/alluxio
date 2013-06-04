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
import tachyon.thrift.Command;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

public class WorkerStorage {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final CommonConf COMMON_CONF;

  private volatile MasterClient mMasterClient;
  private InetSocketAddress mMasterAddress;
  private InetSocketAddress mWorkerAddress;
  private WorkerSpaceCounter mWorkerSpaceCounter;
  private long mWorkerId;

  private Set<Integer> mMemoryData = new HashSet<Integer>();
  private Map<Integer, Long> mFileSizes = new HashMap<Integer, Long>();
  private Map<Integer, Long> mLatestFileAccessTimeMs = new HashMap<Integer, Long>();

  private Map<Integer, Set<Long>> mUsersPerLockedFile = new HashMap<Integer, Set<Long>>();
  private Map<Long, Set<Integer>> mLockedFilesPerUser = new HashMap<Long, Set<Integer>>();

  private BlockingQueue<Integer> mRemovedFileList = 
      new ArrayBlockingQueue<Integer>(Constants.WORKER_FILES_QUEUE_SIZE);
  private BlockingQueue<Integer> mAddedFileList = 
      new ArrayBlockingQueue<Integer>(Constants.WORKER_FILES_QUEUE_SIZE);

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
        mMasterClient.open();
        mWorkerId = mMasterClient.worker_register(
            new NetAddress(mWorkerAddress.getHostName(), mWorkerAddress.getPort()),
            mWorkerSpaceCounter.getCapacityBytes(), 0, new ArrayList<Integer>());
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
    mUnderFs = UnderFileSystem.getUnderFileSystem(COMMON_CONF.UNDERFS_ADDRESS);
    mUsers = new Users(mLocalUserFolder.toString(), mUnderfsWorkerFolder);

    try {
      initializeWorkerStorage();
    } catch (FileDoesNotExistException e) {
      CommonUtils.runtimeException(e);
    } catch (SuspectedFileSizeException e) {
      CommonUtils.runtimeException(e);
    } catch (TException e) {
      CommonUtils.runtimeException(e);
    }

    LOG.info("Current Worker Info: ID " + mWorkerId + ", ADDRESS: " + mWorkerAddress +
        ", MemoryCapacityBytes: " + mWorkerSpaceCounter.getCapacityBytes());
  }

  public void accessFile(int fileId) {
    synchronized (mLatestFileAccessTimeMs) {
      mLatestFileAccessTimeMs.put(fileId, System.currentTimeMillis());
    }
  }

  public void addCheckpoint(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, 
      FailedToCheckpointException, TException {
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

  private void addFoundPartition(int fileId, long fileSizeBytes)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    addId(fileId, fileSizeBytes);
    mMasterClient.worker_cachedFile(mWorkerId, mWorkerSpaceCounter.getUsedBytes(), fileId,
        fileSizeBytes);
  }

  private void addId(int fileId, long fileSizeBytes) {
    synchronized (mLatestFileAccessTimeMs) {
      mLatestFileAccessTimeMs.put(fileId, System.currentTimeMillis());
      mFileSizes.put(fileId, fileSizeBytes);
      mMemoryData.add(fileId);
    }
  }

  public void cacheFile(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    File srcFile = new File(getUserTempFolder(userId) + "/" + fileId);
    File dstFile = new File(mLocalDataFolder + "/" + fileId);
    long fileSizeBytes = srcFile.length(); 
    if (!srcFile.exists()) {
      throw new FileDoesNotExistException("File " + srcFile + " does not exist.");
    }
    if (!srcFile.renameTo(dstFile)) {
      throw new FileDoesNotExistException("Failed to rename file from " + srcFile.getPath() +
          " to " + dstFile.getPath());
    }
    addId(fileId, fileSizeBytes);
    mUsers.addOwnBytes(userId, - fileSizeBytes);
    mMasterClient.worker_cachedFile(mWorkerId, 
        mWorkerSpaceCounter.getUsedBytes(), fileId, fileSizeBytes);
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
      synchronized (mUsersPerLockedFile) {
        Set<Integer> fileIds = mLockedFilesPerUser.get(userId);
        mLockedFilesPerUser.remove(userId);
        if (fileIds != null) {
          for (int fileId : fileIds) {
            try {
              unlockFile(fileId, userId);
            } catch (TException e) {
              CommonUtils.runtimeException(e);
            }
          }
        }
      }
    }
  }

  /**
   * Remove a file from the memory.
   * @param fileId The file to be removed.
   * @return Removed file size in bytes.
   */
  private synchronized long freeFile(int fileId) {
    Long freedFileBytes = null;
    if (mFileSizes.containsKey(fileId)) {
      mWorkerSpaceCounter.returnUsedBytes(mFileSizes.get(fileId));
      File srcFile = new File(mLocalDataFolder + "/" + fileId);
      srcFile.delete();
      synchronized (mLatestFileAccessTimeMs) {
        mLatestFileAccessTimeMs.remove(fileId);
        freedFileBytes = mFileSizes.remove(fileId);
        mRemovedFileList.add(fileId);
        mMemoryData.remove(fileId);
      }
      LOG.info("Removed Data " + fileId);
    } else {
      LOG.warn("File " + fileId + " does not exist in memory.");
    }

    return freedFileBytes == null ? 0 : freedFileBytes;
  }

  /**
   * Remove files from the memory.
   * @param files The list of files to be removed.
   */
  public void freeFiles(List<Integer> files) {
    for (int fileId: files) {
      freeFile(fileId);
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

  public Command heartbeat() throws TException {
    ArrayList<Integer> sendRemovedPartitionList = new ArrayList<Integer>();
    while (mRemovedFileList.size() > 0) {
      sendRemovedPartitionList.add(mRemovedFileList.poll());
    }
    return mMasterClient.worker_heartbeat(mWorkerId, mWorkerSpaceCounter.getUsedBytes(),
        sendRemovedPartitionList);
  }

  private void initializeWorkerStorage() 
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
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

        int fileId = CommonUtils.getFileIdFromFileName(tFile.getName());
        boolean success = mWorkerSpaceCounter.requestSpaceBytes(tFile.length());
        addFoundPartition(fileId, tFile.length());
        mAddedFileList.add(fileId);
        if (!success) {
          CommonUtils.runtimeException("Pre-existing files exceed the local memory capacity.");
        }
      }
    }
  }

  public void lockFile(int fileId, long userId) throws TException {
    synchronized (mUsersPerLockedFile) {
      if (!mUsersPerLockedFile.containsKey(fileId)) {
        mUsersPerLockedFile.put(fileId, new HashSet<Long>());
      }
      mUsersPerLockedFile.get(fileId).add(userId);

      if (!mLockedFilesPerUser.containsKey(userId)) {
        mLockedFilesPerUser.put(userId, new HashSet<Integer>());
      }
      mLockedFilesPerUser.get(userId).add(fileId);
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

    synchronized (mLatestFileAccessTimeMs) {
      synchronized (mUsersPerLockedFile) {
        while (mWorkerSpaceCounter.getAvailableBytes() < requestBytes) {
          int fileId = -1;
          long latestTimeMs = Long.MAX_VALUE;
          for (Entry<Integer, Long> entry : mLatestFileAccessTimeMs.entrySet()) {
            if (entry.getValue() < latestTimeMs && !pinList.contains(entry.getKey())) {
              if(!mUsersPerLockedFile.containsKey(entry.getKey())) {
                fileId = entry.getKey();
                latestTimeMs = entry.getValue();
              }
            }
          }
          if (fileId != -1) {
            freeFile(fileId);
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
        mMasterClient.open();
        id = mMasterClient.worker_register(
            new NetAddress(mWorkerAddress.getHostName(), mWorkerAddress.getPort()),
            mWorkerSpaceCounter.getCapacityBytes(), 0, new ArrayList<Integer>(mMemoryData));
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
    tMasterClient.open();
    mMasterClient = tMasterClient;
  }

  public void unlockFile(int fileId, long userId) throws TException {
    synchronized (mUsersPerLockedFile) {
      if (mUsersPerLockedFile.containsKey(fileId)) {
        mUsersPerLockedFile.get(fileId).remove(userId);
        if (mUsersPerLockedFile.get(fileId).size() == 0) {
          mUsersPerLockedFile.remove(fileId);
        }
      }

      if (mLockedFilesPerUser.containsKey(userId)) {
        mLockedFilesPerUser.get(userId).remove(fileId);
      }
    }
  }

  public void userHeartbeat(long userId) throws TException {
    mUsers.userHeartbeat(userId);
  }
}