package tachyon;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.thrift.TException;
import org.apache.log4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import tachyon.thrift.Command;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.WorkerService;

public class WorkerServiceHandler implements WorkerService.Iface {
  // TODO The reason this is public is for DataServerMessage to access. Need to re-organize this.
  public static final BlockingQueue<Integer> sDataAccessQueue = 
      new ArrayBlockingQueue<Integer>(Config.WORKER_DATA_ACCESS_QUEUE_SIZE);

  private final Logger LOG = Logger.getLogger(Config.LOGGER_TYPE);

  private volatile MasterClient mMasterClient;
  private InetSocketAddress mMasterAddress;
  private WorkerInfo mWorkerInfo;

  // TODO Should merge these three structures, and make it more clean. Define NodeStroage class.
  private Set<Integer> mMemoryData = new HashSet<Integer>();
  private Map<Integer, Long> mLatestFileAccessTimeMs = new HashMap<Integer, Long>();
  private Map<Integer, Set<Long>> mUsersPerLockedFile = new HashMap<Integer, Set<Long>>();
  private Map<Long, Set<Integer>> mLockedFilesPerUser = new HashMap<Long, Set<Integer>>();
  private Map<Integer, Long> mFileSizes = new HashMap<Integer, Long>();
  private BlockingQueue<Integer> mRemovedFileList = 
      new ArrayBlockingQueue<Integer>(Config.WORKER_DATA_ACCESS_QUEUE_SIZE);
  private BlockingQueue<Integer> mAddedFileList = 
      new ArrayBlockingQueue<Integer>(Config.WORKER_DATA_ACCESS_QUEUE_SIZE);

  // Dependency related lock
  private Object mDependencyLock = new Object();
  private Set<Integer> mUncheckpointFiles = new HashSet<Integer>();
  // From dependencyId to files in that set.
  private Map<Integer, Set<Integer>> mDepIdToFiles = new HashMap<Integer, Set<Integer>>();
  private List<Integer> mPriorityDependencies = new ArrayList<Integer>();

  private File mDataFolder;
  private File mUserFolder;
  private Path mHdfsWorkerFolder;
  private Path mHdfsWorkerDataFolder;
  private HdfsClient mHdfsClient;

  private Users mUsers;

  private ArrayList<Thread> mCheckpointThreads = 
      new ArrayList<Thread>(Config.WORKER_CHECKPOINT_THREADS); 

  public class CheckpointThread implements Runnable {
    private final Logger LOG = Logger.getLogger(Config.LOGGER_TYPE);
    private final int ID;
    private HdfsClient mLocalHdfsClient = null;

    public CheckpointThread(int id) {
      ID = id;
    }

    @Override
    public void run() {
      while (true) {
        try {
          int fileId = -1;
          synchronized (mDependencyLock) {
            fileId = getFileIdBasedOnPriorityDependency();

            if (mPriorityDependencies.size() == 0) {
              mPriorityDependencies = getSortedPriorityDependencyList();
              if (!mPriorityDependencies.isEmpty()) {
                LOG.info("Get new mPriorityDependencies " +
                    CommonUtils.listToString(mPriorityDependencies));
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

            if (fileId == -1) {
              fileId = getRandomUncheckpointedFile();
            }
          }

          if (fileId == -1) {
            LOG.debug("Thread " + ID + " has nothing to checkpoint. Sleep for 1 sec.");
            CommonUtils.sleepMs(LOG, 1000);
            continue;
          }

          //TODO checkpoint process. In future, move from midPath to dstPath should be done by
          // master
          String srcPath = mDataFolder + "/" + fileId;
          long fileSize = (new File(srcPath)).length();
          String midPath = mHdfsWorkerDataFolder + "/" + fileId;
          String dstPath = Config.HDFS_ADDRESS + Config.HDFS_DATA_FOLDER + "/" + fileId;
          LOG.info("Thread " + ID + " is checkpointing file " + fileId + " from " + srcPath + 
              " to " + midPath + " to " + dstPath);

          if (mLocalHdfsClient == null) {
            mLocalHdfsClient = new HdfsClient(midPath);
          }

          long startCopyTimeMs = System.currentTimeMillis();
          mLocalHdfsClient.copyFromLocalFile(false, false, srcPath, midPath);
          if (!mLocalHdfsClient.rename(midPath, dstPath)) {
            LOG.error("Failed to rename from " + midPath + " to " + dstPath);
          }
          mMasterClient.addCheckpoint(mWorkerInfo.getId(), fileId, fileSize, dstPath);
          long shouldTakeMs = (long) 
              (1000.0 * fileSize / Config.MB / Config.WORKER_PER_THREAD_CHECKPOINT_CAP_MB_SEC);
          long currentTimeMs = System.currentTimeMillis();
          if (startCopyTimeMs + shouldTakeMs > currentTimeMs) {
            long shouldSleepMs = startCopyTimeMs + shouldTakeMs - currentTimeMs;
            LOG.info("Checkpointed last file " + fileId + " took " + 
                (currentTimeMs - startCopyTimeMs) + " ms. Need to sleep " + shouldSleepMs + " ms.");
            CommonUtils.sleepMs(LOG, shouldSleepMs);
          }

          unlockFile(fileId, Users.sCHECKPOINT_USER_ID); 
        } catch (FileDoesNotExistException e) {
          LOG.warn(e);
        } catch (SuspectedFileSizeException e) {
          LOG.error(e);
        } catch (TException e) {
          LOG.warn(e); 
        }
      }
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

    // This method assumes the mDependencyLock has been acquired.
    private int getRandomUncheckpointedFile() throws TException {
      if (mUncheckpointFiles.isEmpty()) {
        return -1;
      }
      for (int depId: mDepIdToFiles.keySet()) {
        int fileId = getFileIdFromOneDependency(depId);
        if (fileId != -1) {
          return fileId;
        }
      }
      return -1;
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
        lockFile(fileId, Users.sCHECKPOINT_USER_ID);
        fileIds.remove(fileId);
        mUncheckpointFiles.remove(fileId);
        if (fileIds.isEmpty()) {
          mDepIdToFiles.remove(depId);
        }
        return fileId;
      }
      return -1;
    }
  }

  public WorkerServiceHandler(InetSocketAddress masterAddress, InetSocketAddress workerAddress,
      String dataFolder, long spaceLimitBytes) {
    mMasterAddress = masterAddress;
    mMasterClient = new MasterClient(mMasterAddress);

    long id = 0;
    while (id == 0) {
      try {
        mMasterClient.open();
        id = mMasterClient.worker_register(
            new NetAddress(workerAddress.getHostName(), workerAddress.getPort()),
            spaceLimitBytes, 0, new ArrayList<Integer>());
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleepMs(LOG, 1000);
      }
    }

    mDataFolder = new File(dataFolder);
    mUserFolder = new File(mDataFolder.toString(), Config.USER_TEMP_RELATIVE_FOLDER);
    mWorkerInfo = new WorkerInfo(id, workerAddress, spaceLimitBytes);
    mHdfsWorkerFolder = new Path(Config.HDFS_ADDRESS + "/" + Config.WORKER_HDFS_FOLDER + "/" + id);
    mHdfsWorkerDataFolder = new Path(mHdfsWorkerFolder.toString() + "/data");
    if (Config.USING_HDFS) {
      mHdfsClient = new HdfsClient(Config.HDFS_ADDRESS);
    }
    mUsers = new Users(mUserFolder.toString(), mHdfsWorkerFolder.toString());

    if (Config.USING_HDFS) {
      for (int k = 0; k < Config.WORKER_CHECKPOINT_THREADS; k ++) {
        Thread thread = new Thread(new CheckpointThread(k));
        mCheckpointThreads.add(thread);
        thread.start();
      }
    }

    try {
      initializeWorkerInfo();
    } catch (FileDoesNotExistException e) {
      CommonUtils.runtimeException(e);
    } catch (SuspectedFileSizeException e) {
      CommonUtils.runtimeException(e);
    } catch (TException e) {
      CommonUtils.runtimeException(e);
    }

    LOG.info("Current Worker Info: " + mWorkerInfo);
  }

  @Override
  public void accessFile(int fileId) throws TException {
    sDataAccessQueue.add(fileId);
  }

  @Override
  public void addCheckpoint(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, 
      FailedToCheckpointException, TException {
    // TODO This part need to be changed.
    String srcPath = getUserHdfsTempFolder(userId) + "/" + fileId;
    String dstPath = Config.HDFS_ADDRESS + Config.HDFS_DATA_FOLDER + "/" + fileId;
    if (!mHdfsClient.rename(srcPath, dstPath)) {
      throw new FailedToCheckpointException("Failed to rename from " + srcPath + " to " + dstPath);
    }
    long fileSize = mHdfsClient.getFileSize(dstPath);
    mMasterClient.addCheckpoint(mWorkerInfo.getId(), fileId, fileSize, dstPath);
  }

  private void addFoundPartition(int fileId, long fileSizeBytes)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    addId(fileId, fileSizeBytes);
    mMasterClient.worker_cachedFile(mWorkerInfo.getId(), mWorkerInfo.getUsedBytes(), fileId,
        fileSizeBytes);
  }

  private void addId(int fileId, long fileSizeBytes) {
    mWorkerInfo.updateFile(true, fileId);

    synchronized (mLatestFileAccessTimeMs) {
      mLatestFileAccessTimeMs.put(fileId, System.currentTimeMillis());
      mFileSizes.put(fileId, fileSizeBytes);
      mMemoryData.add(fileId);
    }
  }

  @Override
  public void cacheFile(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    File srcFile = new File(getUserTempFolder(userId) + "/" + fileId);
    File dstFile = new File(mDataFolder + "/" + fileId);
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
    int dependencyId = mMasterClient.worker_cachedFile(mWorkerInfo.getId(), 
        mWorkerInfo.getUsedBytes(), fileId, fileSizeBytes);

    if (dependencyId != -1) {
      synchronized (mDependencyLock) {
        mUncheckpointFiles.add(fileId);
        if (!mDepIdToFiles.containsKey(dependencyId)) {
          mDepIdToFiles.put(dependencyId, new HashSet<Integer>());
        }
        mDepIdToFiles.get(dependencyId).add(fileId);
      }
    }
  }

  public void checkStatus() {
    List<Long> removedUsers = mUsers.checkStatus(mWorkerInfo);

    for (long userId : removedUsers) {
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

    synchronized (mLatestFileAccessTimeMs) {
      while (!sDataAccessQueue.isEmpty()) {
        int fileId = sDataAccessQueue.poll();

        mLatestFileAccessTimeMs.put(fileId, System.currentTimeMillis());
      }
    }
  }

  private void freeFile(int fileId) {
    mWorkerInfo.returnUsedBytes(mFileSizes.get(fileId));
    mWorkerInfo.removeFile(fileId);
    File srcFile = new File(mDataFolder + "/" + fileId);
    srcFile.delete();
    synchronized (mLatestFileAccessTimeMs) {
      mLatestFileAccessTimeMs.remove(fileId);
      mFileSizes.remove(fileId);
      mRemovedFileList.add(fileId);
      mMemoryData.remove(fileId);
    }
    LOG.info("Removed Data " + fileId);
  }

  @Override
  public String getDataFolder() throws TException {
    return mDataFolder.toString();
  }

  @Override
  public String getUserTempFolder(long userId) throws TException {
    String ret = mUsers.getUserTempFolder(userId);
    LOG.info("Return UserTempFolder for " + userId + " : " + ret);
    return ret;
  }

  @Override
  public String getUserHdfsTempFolder(long userId) throws TException {
    String ret = mUsers.getUserHdfsTempFolder(userId);
    LOG.info("Return UserHdfsTempFolder for " + userId + " : " + ret);
    return ret;
  }

  public Command heartbeat() throws TException {
    ArrayList<Integer> sendRemovedPartitionList = new ArrayList<Integer>();
    while (mRemovedFileList.size() > 0) {
      sendRemovedPartitionList.add(mRemovedFileList.poll());
    }
    return mMasterClient.worker_heartbeat(mWorkerInfo.getId(), mWorkerInfo.getUsedBytes(),
        sendRemovedPartitionList);
  }

  private void initializeWorkerInfo() 
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    LOG.info("Initializing the worker info.");
    if (!mDataFolder.exists()) {
      LOG.info("Local folder " + mDataFolder.toString() + " does not exist. Creating a new one.");

      mDataFolder.mkdir();
      mUserFolder.mkdir();

      return;
    }

    if (!mDataFolder.isDirectory()) {
      String tmp = mDataFolder.toString() + " is not a folder!";
      LOG.error(tmp);
      throw new IllegalArgumentException(tmp);
    }

    int cnt = 0;
    for (File tFile : mDataFolder.listFiles()) {
      if (tFile.isFile()) {
        cnt ++;
        LOG.info("File " + cnt + ": " + tFile.getPath() + " with size " + tFile.length() + " Bs.");

        int fileId = CommonUtils.getFileIdFromFileName(tFile.getName());
        boolean success = mWorkerInfo.requestSpaceBytes(tFile.length());
        addFoundPartition(fileId, tFile.length());
        mAddedFileList.add(fileId);
        if (!success) {
          CommonUtils.runtimeException("Pre-existing files exceed the local memory capacity.");
        }
      }
    }

    if (mUserFolder.exists()) {
      try {
        FileUtils.deleteDirectory(mUserFolder);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    mUserFolder.mkdir();
  }

  @Override
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

  private boolean memoryEvictionLRU() {
    long latestTimeMs = Long.MAX_VALUE;
    int fileId = -1;
    Set<Integer> pinList = new HashSet<Integer>();

    // TODO Cache replacement policy should go through Master.
    try {
      pinList = mMasterClient.worker_getPinIdList();
    } catch (TException e) {
      LOG.error(e.getMessage());
      pinList = new HashSet<Integer>();
    }

    synchronized (mLatestFileAccessTimeMs) {
      synchronized (mUsersPerLockedFile) {
        synchronized (mDependencyLock) {
          for (Entry<Integer, Long> entry : mLatestFileAccessTimeMs.entrySet()) {
            if (entry.getValue() < latestTimeMs && !pinList.contains(entry.getKey())) {
              if(!mUsersPerLockedFile.containsKey(entry.getKey())
                  && !mUncheckpointFiles.contains(entry.getKey())) {
                fileId = entry.getKey();
                latestTimeMs = entry.getValue();
              }
            }
          }
          if (fileId != -1) {
            freeFile(fileId);
            return true;
          }
        }
      }
    }

    return false;
  }

  public void register() {
    long id = 0;
    while (id == 0) {
      try {
        mMasterClient.open();
        id = mMasterClient.worker_register(
            new NetAddress(mWorkerInfo.ADDRESS.getHostName(), mWorkerInfo.ADDRESS.getPort()),
            mWorkerInfo.getCapacityBytes(), 0, new ArrayList<Integer>());
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleepMs(LOG, 1000);
      }
    }
    mWorkerInfo.updateId(id);
  }

  @Override
  public void returnSpace(long userId, long returnedBytes) throws TException {
    LOG.info("returnSpace(" + userId + ", " + returnedBytes + ") : " +
        mWorkerInfo.getAvailableBytes() + " returned: " + returnedBytes);

    mWorkerInfo.returnUsedBytes(returnedBytes);
    mUsers.addOwnBytes(userId, - returnedBytes);

    LOG.info("user_returnSpace(): new available: " + mWorkerInfo.getAvailableBytes());
  }

  @Override
  public boolean requestSpace(long userId, long requestBytes) throws TException {
    LOG.info("requestSpace(" + userId + ", " + requestBytes + "): Current available: " +
        mWorkerInfo.getAvailableBytes() + " requested: " + requestBytes);
    if (mWorkerInfo.getCapacityBytes() < requestBytes) {
      LOG.info("user_requestSpace(): requested memory size is larger than the total memory on" +
          " the machine.");
      return false;
    }

    while (!mWorkerInfo.requestSpaceBytes(requestBytes)) {
      if (!memoryEvictionLRU()) {
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

  @Override
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

  @Override
  public void userHeartbeat(long userId) throws TException {
    mUsers.userHeartbeat(userId);
  }
}