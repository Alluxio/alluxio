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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.thrift.PartitionAlreadyExistException;
import tachyon.thrift.PartitionDoesNotExistException;
import tachyon.thrift.SuspectedPartitionSizeException;
import tachyon.thrift.WorkerService;

public class WorkerServiceHandler implements WorkerService.Iface {
  // TODO The reason this is public is for DataServerMessage to access. Need to re-organize this.
  public static final BlockingQueue<Integer> sDataAccessQueue = 
      new ArrayBlockingQueue<Integer>(Config.WORKER_DATA_ACCESS_QUEUE_SIZE);

  private final Logger LOG = LoggerFactory.getLogger(WorkerServiceHandler.class);

  private volatile MasterClient mMasterClient;
  private InetSocketAddress mMasterAddress;
  private WorkerInfo mWorkerInfo;

  // TODO Should merge these three structures, and make it more clean. Define NodeStroage class.
  private Map<Integer, Set<Integer>> mMemoryData = new HashMap<Integer, Set<Integer>>();
  private Map<Long, Long> mLatestPartitionAccessTime = new HashMap<Long, Long>();
  private Map<Long, Set<Long>> mLockedPartitionPerPartition = new HashMap<Long, Set<Long>>();
  private Map<Long, Set<Long>> mLockedPartitionPerUser = new HashMap<Long, Set<Long>>();
  private Map<Long, Long> mPartitionSizes = new HashMap<Long, Long>();
  private BlockingQueue<Long> mRemovedPartitionList = 
      new ArrayBlockingQueue<Long>(Config.WORKER_DATA_ACCESS_QUEUE_SIZE);
  private BlockingQueue<Long> mAddedPartitionList = 
      new ArrayBlockingQueue<Long>(Config.WORKER_DATA_ACCESS_QUEUE_SIZE);
  private File mDataFolder;
  private File mUserFolder;
  private Path mHdfsWorkerFolder;
  private HdfsClient mHdfsClient;

  private Users mUsers;

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
            spaceLimitBytes, 0, new ArrayList<Long>());
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleep(1000);
      }
    }

    mDataFolder = new File(dataFolder);
    mUserFolder = new File(mDataFolder.toString(), Config.USER_TEMP_RELATIVE_FOLDER);
    mWorkerInfo = new WorkerInfo(id, workerAddress, spaceLimitBytes);
    mHdfsWorkerFolder = new Path(Config.HDFS_ADDRESS + "/" + Config.WORKER_HDFS_FOLDER + "/" + id);
    if (Config.USING_HDFS) {
      mHdfsClient = new HdfsClient(Config.HDFS_ADDRESS);
    }
    mUsers = new Users(mUserFolder.toString(), mHdfsWorkerFolder.toString());

    try {
      initializeWorkerInfo();
    } catch (PartitionDoesNotExistException e) {
      CommonUtils.runtimeException(e);
    } catch (SuspectedPartitionSizeException e) {
      CommonUtils.runtimeException(e);
    } catch (TException e) {
      CommonUtils.runtimeException(e);
    }

    LOG.info("Current Worker Info: " + mWorkerInfo);
  }

  @Override
  public void accessPartition(int datasetId, int partitionId) throws TException {
    sDataAccessQueue.add(CommonUtils.generateBigId(datasetId, partitionId));
  }

  private void addBigId(long bigId, long fileSizeBytes) {
    mWorkerInfo.updatePartition(true, bigId);
    int datasetId = CommonUtils.computeDatasetIdFromBigId(bigId);
    int partitionId = CommonUtils.computePartitionIdFromBigId(bigId);

    synchronized (mLatestPartitionAccessTime) {
      mLatestPartitionAccessTime.put(CommonUtils.generateBigId(datasetId, partitionId),
          System.currentTimeMillis());
      mPartitionSizes.put(CommonUtils.generateBigId(datasetId, partitionId), fileSizeBytes);
      if (!mMemoryData.containsKey(datasetId)) {
        mMemoryData.put(datasetId, new HashSet<Integer>());
      }
      mMemoryData.get(datasetId).add(partitionId);
    }
  }

  @Override
  public void addPartition(long userId, int datasetId, int partitionId, boolean writeThrough)
      throws PartitionDoesNotExistException, SuspectedPartitionSizeException, 
      PartitionAlreadyExistException, TException {
    File srcFile = new File(getUserTempFolder(userId) + "/" + datasetId + "-" + partitionId);
    File dstFile = new File(mDataFolder + "/" + datasetId + "-" + partitionId);
    if (dstFile.exists()) {
      throw new PartitionAlreadyExistException("Partition " + datasetId + "-" + partitionId + 
          " already exists.");
    }
    long fileSizeBytes = srcFile.length(); 
    if (!srcFile.renameTo(dstFile)) {
      CommonUtils.runtimeException("Failed to rename file from " + srcFile.getPath() +
          " to " + dstFile.getPath());
    }
    String dstPath = "";
    if (writeThrough) {
      // TODO This part need to be changed.
      String name = datasetId + "-" + partitionId;
      String srcPath = getUserHdfsTempFolder(userId) + "/" + name;
      dstPath = Config.HDFS_ADDRESS + Config.HDFS_DATA_FOLDER + "/" + name;
      if (Config.USING_HDFS) {
        mHdfsClient.mkdirs(Config.HDFS_ADDRESS + Config.HDFS_DATA_FOLDER + "/" , null, true);
        if (!mHdfsClient.rename(srcPath, dstPath)) {
          LOG.error("Failed to rename from " + srcPath + " to " + dstPath);
          dstPath = "";
        }
      }
    }
    addBigId(CommonUtils.generateBigId(datasetId, partitionId), fileSizeBytes);
    mUsers.addOwnBytes(userId, - fileSizeBytes);
    mMasterClient.worker_addPartition(mWorkerInfo.getId(), mWorkerInfo.getUsedBytes(), datasetId,
        partitionId, (int)fileSizeBytes, writeThrough && !dstPath.equals(""), dstPath);
  }

  @Override
  public void addRCDPartition(int datasetId, int partitionId, int partitionSizeBytes)
      throws PartitionDoesNotExistException, SuspectedPartitionSizeException,
      PartitionAlreadyExistException, TException {
    mMasterClient.worker_addRCDPartition(mWorkerInfo.getId(),
        datasetId, partitionId, partitionSizeBytes);
  }

  private void addFoundPartition(int datasetId, int partitionId, long fileSizeBytes)
      throws PartitionDoesNotExistException, SuspectedPartitionSizeException, TException {
    addBigId(CommonUtils.generateBigId(datasetId, partitionId), fileSizeBytes);
    mMasterClient.worker_addPartition(mWorkerInfo.getId(), mWorkerInfo.getUsedBytes(), datasetId,
        partitionId, (int)fileSizeBytes, false, "");
  }

  public void checkStatus() {
    List<Long> removedUsers = mUsers.checkStatus(mWorkerInfo);

    for (long userId : removedUsers) {
      synchronized (mLockedPartitionPerPartition) {
        Set<Long> partitionIds = mLockedPartitionPerUser.get(userId);
        mLockedPartitionPerUser.remove(userId);
        if (partitionIds != null) {
          for (long pId : partitionIds) {
            try {
              unlockPartition(CommonUtils.computeDatasetIdFromBigId(pId),
                  CommonUtils.computePartitionIdFromBigId(pId), userId);
            } catch (TException e) {
              CommonUtils.runtimeException(e);
            }
          }
        }
      }
    }

    synchronized (mLatestPartitionAccessTime) {
      while (!sDataAccessQueue.isEmpty()) {
        long bigId = sDataAccessQueue.poll();

        mLatestPartitionAccessTime.put(bigId, System.currentTimeMillis());
      }
    }
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
    ArrayList<Long> sendRemovedPartitionList = new ArrayList<Long>();
    while (mRemovedPartitionList.size() > 0) {
      sendRemovedPartitionList.add(mRemovedPartitionList.poll());
    }
    return mMasterClient.worker_heartbeat(mWorkerInfo.getId(), mWorkerInfo.getUsedBytes(),
        sendRemovedPartitionList);
  }

  private void initializeWorkerInfo() 
      throws PartitionDoesNotExistException, SuspectedPartitionSizeException, TException {
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

        int datasetId = CommonUtils.getDatasetIdFromFileName(tFile.getName());
        int pId = CommonUtils.getPartitionIdFromFileName(tFile.getName());
        long bigId = CommonUtils.generateBigId(datasetId, pId);
        boolean success = mWorkerInfo.requestSpaceBytes(tFile.length());
        addFoundPartition(datasetId, pId, tFile.length());
        mAddedPartitionList.add(bigId);
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
    long bigId = CommonUtils.generateBigId(datasetId, partitionId);
    synchronized (mLockedPartitionPerPartition) {
      if (!mLockedPartitionPerPartition.containsKey(bigId)) {
        mLockedPartitionPerPartition.put(bigId, new HashSet<Long>());
      }
      mLockedPartitionPerPartition.get(bigId).add(userId);

      if (!mLockedPartitionPerUser.containsKey(userId)) {
        mLockedPartitionPerUser.put(userId, new HashSet<Long>());
      }
      mLockedPartitionPerUser.get(userId).add(bigId);
    }
  }

  private boolean memoryEvictionLRU() {
    long latestTimeMs = Long.MAX_VALUE;
    long bigId = -1;
    Set<Integer> pinList = new HashSet<Integer>();

    // TODO Cache replacement policy should go through Master.
    try {
      pinList = mMasterClient.worker_getPinList();
    } catch (TException e) {
      LOG.error(e.getMessage());
      pinList = new HashSet<Integer>();
    }

    synchronized (mLatestPartitionAccessTime) {
      synchronized (mLockedPartitionPerPartition) {
        for (Entry<Long, Long> entry : mLatestPartitionAccessTime.entrySet()) {
          if (entry.getValue() < latestTimeMs 
              && !pinList.contains(CommonUtils.computeDatasetIdFromBigId(entry.getKey()))) {
            if(!mLockedPartitionPerPartition.containsKey(entry.getKey())) {
              bigId = entry.getKey();
              latestTimeMs = entry.getValue();
            }
          }
        }
        if (bigId != -1) {
          removePartition(bigId);
          return true;
        }
      }
    }

    return false;
  }

  private void removePartition(long bigId) {
    mWorkerInfo.returnUsedBytes(mPartitionSizes.get(bigId));
    mWorkerInfo.removePartition(bigId);
    File srcFile = new File(mDataFolder + "/" + CommonUtils.computeDatasetIdFromBigId(bigId) + "-" + 
        CommonUtils.computePartitionIdFromBigId(bigId));
    srcFile.delete();
    synchronized (mLatestPartitionAccessTime) {
      mLatestPartitionAccessTime.remove(bigId);
      mPartitionSizes.remove(bigId);
      mRemovedPartitionList.add(bigId);
      int datasetId = CommonUtils.computeDatasetIdFromBigId(bigId);
      int partitionId = CommonUtils.computePartitionIdFromBigId(bigId);
      mMemoryData.get(datasetId).remove(partitionId);
    }
    LOG.info("Removed Data " + CommonUtils.computeDatasetIdFromBigId(bigId) + ":" + 
        CommonUtils.computePartitionIdFromBigId(bigId));
  }

  public void register() {
    long id = 0;
    while (id == 0) {
      try {
        mMasterClient.open();
        id = mMasterClient.worker_register(
            new NetAddress(mWorkerInfo.ADDRESS.getHostName(), mWorkerInfo.ADDRESS.getPort()),
            mWorkerInfo.TOTAL_BYTES, 0, new ArrayList<Long>());
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        id = 0;
        CommonUtils.sleep(1000);
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
    if (mWorkerInfo.TOTAL_BYTES < requestBytes) {
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
    long bigId = CommonUtils.generateBigId(datasetId, partitionId);
    synchronized (mLockedPartitionPerPartition) {
      if (mLockedPartitionPerPartition.containsKey(bigId)) {
        mLockedPartitionPerPartition.get(bigId).remove(userId);
        if (mLockedPartitionPerPartition.get(bigId).size() == 0) {
          mLockedPartitionPerPartition.remove(bigId);
        }
      }

      if (mLockedPartitionPerUser.containsKey(userId)) {
        mLockedPartitionPerUser.get(userId).remove(bigId);
      }
    }
  }

  @Override
  public void userHeartbeat(long userId) throws TException {
    mUsers.userHeartbeat(userId);
  }
}