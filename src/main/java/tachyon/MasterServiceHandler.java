package tachyon;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.LogEventType;
import tachyon.thrift.MasterService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * The Master server program.
 * 
 * It maintains the state of each worker. It never keeps the state of any user.
 * 
 * @author haoyuan
 */
public class MasterServiceHandler implements MasterService.Iface {
  private final Logger LOG = LoggerFactory.getLogger(MasterServiceHandler.class);
  private final InetSocketAddress ADDRESS;
  private final long START_TIME_NS_PREFIX;
  private final long START_TIME_MS;

  private AtomicInteger mDatasetCounter = new AtomicInteger(0);
  private AtomicInteger mUserCounter = new AtomicInteger(0);
  private AtomicInteger mWorkerCounter = new AtomicInteger(0);

  // TODO Merge the following strcuture into a structure?
  private Map<Integer, INode> mFiles = new HashMap<Integer, INode>();
  private Map<String, Integer> mFilePathToId = new HashMap<String, Integer>();

  //  private Map<Integer, RawColumnDatasetInfo> mRawColumnDatasets = 
  //      new HashMap<Integer, RawColumnDatasetInfo>();
  //  private Map<String, Integer> mRawColumnDatasetPathToId = new HashMap<String, Integer>();

  private Map<Long, WorkerInfo> mWorkers = new HashMap<Long, WorkerInfo>();
  private Map<InetSocketAddress, Long> mWorkerAddressToId = new HashMap<InetSocketAddress, Long>();
  private BlockingQueue<WorkerInfo> mLostWorkers = new ArrayBlockingQueue<WorkerInfo>(32);

  // Fault Recovery Log
  private MasterLogWriter mMasterLogWriter;

  private Thread mHeartbeatThread;

  private PrefixList mWhiteList;
  private PrefixList mPinList;
  private List<Integer> mIdPinList;

  private WebServer mWebServer;

  /**
   * System periodical status check.
   * 
   * @author Haoyuan
   */
  public class MasterHeartbeatExecutor implements HeartbeatExecutor {
    public MasterHeartbeatExecutor() {
    }

    @Override
    public void heartbeat() {
      LOG.info("Periodical system status checking...");

      Set<Long> lostWorkers = new HashSet<Long>();

      synchronized (mWorkers) {
        for (Entry<Long, WorkerInfo> worker: mWorkers.entrySet()) {
          if (CommonUtils.getCurrentMs() - worker.getValue().getLastUpdatedTimeMs() 
              > Config.WORKER_TIMEOUT_MS) {
            LOG.error("The worker " + worker.getKey() + " ( " + worker.getValue() + 
                " ) got timed out!");
            mLostWorkers.add(worker.getValue());
            lostWorkers.add(worker.getKey());
          }
        }
        for (long workerId: lostWorkers) {
          mWorkers.remove(workerId);
        }
      }

      boolean hadFailedWorker = false;

      while (mLostWorkers.size() != 0) {
        hadFailedWorker = true;
        WorkerInfo worker = mLostWorkers.poll();

        for (int id: worker.getFiles()) {
          synchronized (mFiles) {
            INode tFile = mFiles.get(id);
            if (tFile != null) {
              Map<Long, NetAddress> locations = tFile.mLocations;

              if (locations.containsKey(worker.getId())) {
                locations.remove(worker.getId());
              }
            }
          }
        }
      }

      if (hadFailedWorker) {
        LOG.warn("Restarting failed workers");
        try {
          java.lang.Runtime.getRuntime().exec(Config.TACHYON_HOME + 
              "/bin/restart-failed-tachyon-workers.sh");
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }

  public MasterServiceHandler(InetSocketAddress address) {
    START_TIME_MS = System.currentTimeMillis();
    // TODO This name need to be changed.
    START_TIME_NS_PREFIX = START_TIME_MS - (START_TIME_MS % 1000000);
    ADDRESS = address;

    mWhiteList = new PrefixList(Config.WHITELIST);
    mPinList = new PrefixList(Config.PINLIST);
    mIdPinList = Collections.synchronizedList(new ArrayList<Integer>());

    // TODO Fault recovery: need user counter info;
    recoveryFromLog();
    writeCheckpoint();

    if (Config.MASTER_SUBSUME_HDFS) {
      HdfsClient hdfs = new HdfsClient(Config.HDFS_ADDRESS);
      //      subsumeHdfs(hdfs, Config.HDFS_ADDRESS);
    }

    mMasterLogWriter = new MasterLogWriter(Config.MASTER_LOG_FILE);

    mHeartbeatThread = new Thread(new HeartbeatThread(
        new MasterHeartbeatExecutor(), Config.MASTER_HEARTBEAT_INTERVAL_MS));
    mHeartbeatThread.start();

    mWebServer = new WebServer("Tachyon Master Server",
        new InetSocketAddress(address.getHostName(), Config.MASTER_WEB_PORT));
    mWebServer.setHandler(new WebServerMasterHandler(this));
    mWebServer.startWebServer();
  }

  @Override
  public List<String> cmd_ls(String folder) throws TException {
    ArrayList<String> ret = new ArrayList<String>();
    synchronized (mFiles) {
      for (INode fileInfo : mFiles.values()) {
        if (fileInfo.getName().startsWith(folder)) {
          ret.add(fileInfo.getName());
        }
      }

      return ret;
    }
  }

  public long getStarttimeMs() {
    return START_TIME_MS;
  }

  public String toHtml() {
    long timeMs = System.currentTimeMillis() - START_TIME_MS;
    StringBuilder sb = new StringBuilder("<h1> Tachyon has been running @ " + ADDRESS + 
        " for " + CommonUtils.convertMillis(timeMs) + " </h1> \n");

    sb.append(mWhiteList.toHtml("WhiteList"));

    sb.append(mPinList.toHtml("PinList"));

    synchronized (mWorkers) {
      synchronized (mFiles) {
        sb.append("<h2>" + mWorkers.size() + " worker(s) are running: </h2>");
        List<Long> workerList = new ArrayList<Long>(mWorkers.keySet());
        Collections.sort(workerList);
        for (int k = 0; k < workerList.size(); k ++) {
          sb.append("<strong>Worker " + (k + 1) + " </strong>: " + 
              mWorkers.get(workerList.get(k)).toHtml() + "<br \\>");
        }

        sb.append("<h2>" + mFiles.size() + " File(s): </h2>");
        List<Integer> fileIdList = new ArrayList<Integer>(mFiles.keySet());
        Collections.sort(fileIdList);
        for (int k = 0; k < fileIdList.size(); k ++) {
          INode tINode = mFiles.get(fileIdList.get(k));
          sb.append("<strong>File " + (k + 1) + " </strong>: ");
          sb.append("ID: ").append(tINode.mId).append("; ");
          sb.append("Path: ").append(tINode.mName).append("; ");
          if (Config.DEBUG) {
            sb.append(tINode.toString());
          }
          sb.append("<br \\>");
        }
      }
    }

    return sb.toString();
  }

  @Override
  public int user_createFile(String filePath)
      throws FileAlreadyExistException, TException, InvalidPathException {
    return user_createFile(filePath, false, -1);
  }

  public int user_createFile(String filePath, boolean isSubDataset, int parentFileId)
      throws FileAlreadyExistException, TException, InvalidPathException {
    String parameters = CommonUtils.parametersToString(filePath);
    LOG.info("user_createFile" + parameters);

    INode inode = null;

    synchronized (mFiles) {
      if (mFilePathToId.containsKey(filePath)) {
        LOG.info(filePath + " already exists.");
        throw new FileAlreadyExistException("File " + filePath + " already exists.");
      }

      inode = new INodeFile(filePath, mDatasetCounter.addAndGet(1), parentFileId);
      inode.mPin = mPinList.inList(filePath);
      inode.mCache = mWhiteList.inList(filePath);

      mMasterLogWriter.appendAndFlush(inode);

      mFilePathToId.put(filePath, inode.mId);
      mFiles.put(inode.mId, inode);

      if (mPinList.inList(filePath)) {
        mIdPinList.add(inode.mId);
      }

      LOG.info("user_createFile: File Created: " + inode);
    }

    return inode.mId;
  }

  //  @Override
  //  public int user_createRawColumnDataset(String datasetPath, int columns, int partitions)
  //      throws FileAlreadyExistException, InvalidPathException, TException {
  //    String parameters = CommonUtils.parametersToString(datasetPath, columns, partitions);
  //    LOG.info("user_createRawColumnDataset" + parameters);
  //
  //    RawColumnDatasetInfo rawColumnDataset = null;
  //
  //    synchronized (mDatasets) {
  //      if (mDatasetPathToId.containsKey(datasetPath) 
  //          || mRawColumnDatasetPathToId.containsKey(datasetPath)) {
  //        LOG.info(datasetPath + " already exists.");
  //        throw new FileAlreadyExistException("RawColumnDataset " + datasetPath + 
  //            " already exists.");
  //      }
  //
  //      rawColumnDataset = new RawColumnDatasetInfo();
  //      rawColumnDataset.mId = mDatasetCounter.addAndGet(1);
  //      rawColumnDataset.mPath = datasetPath;
  //      rawColumnDataset.mColumns = columns;
  //      rawColumnDataset.mNumOfPartitions = partitions;
  //      rawColumnDataset.mColumnDatasetIdList = new ArrayList<Integer>(columns);
  //      for (int k = 0; k < columns; k ++) {
  //        rawColumnDataset.mColumnDatasetIdList.add(
  //            user_createDataset(datasetPath + "/col_" + k, partitions, true, rawColumnDataset.mId));
  //      }
  //      rawColumnDataset.mPartitionList = new ArrayList<PartitionInfo>(partitions);
  //      for (int k = 0; k < partitions; k ++) {
  //        PartitionInfo partition = new PartitionInfo();
  //        partition.mDatasetId = rawColumnDataset.mId;
  //        partition.mPartitionId = k;
  //        partition.mSizeBytes = -1;
  //        partition.mLocations = new HashMap<Long, NetAddress>();
  //        rawColumnDataset.mPartitionList.add(partition);
  //      }
  //
  //      mMasterLogWriter.appendAndFlush(rawColumnDataset);
  //
  //      mRawColumnDatasetPathToId.put(datasetPath, rawColumnDataset.mId);
  //      mRawColumnDatasets.put(rawColumnDataset.mId, rawColumnDataset);
  //
  //      LOG.info("user_createRawColumnDataset: RawColumnDataset Created: " + rawColumnDataset);
  //    }
  //
  //    return rawColumnDataset.mId;
  //  }

  @Override
  public void user_deleteById(int fileId) throws FileDoesNotExistException, TException {
    LOG.info("user_deleteById(" + fileId + ")");
    // Only remove meta data from master. The data in workers will be evicted since no further
    // application can read them. (Based on LRU) TODO May change it to be active from V0.2. 
    synchronized (mFiles) {
      if (!mFiles.containsKey(fileId)) {
        throw new FileDoesNotExistException("Failed to delete " + fileId);
      }

      synchronized (mIdPinList) {
        mIdPinList.remove(fileId);
      }
      INode inode = mFiles.remove(fileId);
      mFilePathToId.remove(inode.mName);
      inode.mId = - inode.mId;

      mMasterLogWriter.appendAndFlush(inode);
    }
  }

  @Override
  public void user_deleteByPath(String filePath) throws FileDoesNotExistException, TException {
    LOG.info("user_deleteByPath(" + filePath + ")");
    // Only remove meta data from master. The data in workers will be evicted since no further
    // application can read them. (Based on LRU) TODO May change it to be active from V0.2. 
    synchronized (mFiles) {
      if (!mFilePathToId.containsKey(filePath)) {
        throw new FileDoesNotExistException("Failed to delete " + filePath);
      }

      int id = mFilePathToId.remove(filePath);

      synchronized (mIdPinList) {
        mIdPinList.remove(new Integer(id));
      }
      INode inode = mFiles.remove(id);
      inode.mId = - inode.mId;

      mMasterLogWriter.appendAndFlush(inode);
    }
  }

  //  @Override
  //  public void user_deleteRawColumnDataset(int datasetId)
  //      throws FileDoesNotExistException, TException {
  //    // TODO Auto-generated method stub
  //    LOG.info("user_deleteDataset(" + datasetId + ")");
  //    // Only remove meta data from master. The data in workers will be evicted since no further
  //    // application can read them. (Based on LRU) TODO May change it to be active from V0.2. 
  //    synchronized (mDatasets) {
  //      if (!mRawColumnDatasets.containsKey(datasetId)) {
  //        throw new FileDoesNotExistException("Failed to delete " + datasetId + 
  //            " RawColumnDataset.");
  //      }
  //
  //      RawColumnDatasetInfo dataset = mRawColumnDatasets.remove(datasetId);
  //      mRawColumnDatasetPathToId.remove(dataset.mPath);
  //      dataset.mId = - dataset.mId;
  //
  //      for (int k = 0; k < dataset.mColumns; k ++) {
  //        user_deleteDataset(dataset.mColumnDatasetIdList.get(k));
  //      }
  //
  //      // TODO this order is not right. Move this upper.
  //      mMasterLogWriter.appendAndFlush(dataset);
  //    }
  //  }

  @Override
  public NetAddress user_getLocalWorker(String host)
      throws NoLocalWorkerException, TException {
    LOG.info("user_getLocalWorker(" + host + ")");
    synchronized (mWorkers) {
      for (InetSocketAddress address: mWorkerAddressToId.keySet()) {
        if (address.getHostName().equals(host)) {
          LOG.info("user_getLocalWorker(return good result: " + address);
          return new NetAddress(address.getHostName(), address.getPort());
        }
      }
    }
    LOG.info("user_getLocalWorker: no local worker: " + host + " " + mWorkers.keySet());
    throw new NoLocalWorkerException("user_getLocalWorker(" + host + ") has no local worker.");
  }

  @Override
  public ClientFileInfo user_getClientFileInfoById(int fileId)
      throws FileDoesNotExistException, TException {
    LOG.info("user_getClientFileInfoById(" + fileId + ")");
    synchronized (mFiles) {
      INode inode = mFiles.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      ClientFileInfo ret = new ClientFileInfo();
      ret.id = inode.mId;
      ret.fileName = inode.mName;
      ret.checkpointPath = inode.mCheckpointPath;
      ret.needPin = inode.mPin;
      ret.needCache = inode.mCache;
      LOG.info("user_getClientFileInfoById(" + fileId + "): "  + ret);
      return ret;
    }
  }

  @Override
  public ClientFileInfo user_getClientFileInfoByPath(String filePath)
      throws FileDoesNotExistException, TException {
    LOG.info("user_getClientFileInfoByPath(" + filePath + ")");
    synchronized (mFiles) {
      if (!mFilePathToId.containsKey(filePath)) {
        throw new FileDoesNotExistException("File " + filePath + " does not exist.");
      }

      INode inode = mFiles.get(mFilePathToId.get(filePath));
      ClientFileInfo ret = new ClientFileInfo();
      ret.id = inode.mId;
      ret.fileName = inode.mName;
      ret.checkpointPath = inode.mCheckpointPath;
      ret.needPin = inode.mPin;
      ret.needCache = inode.mCache;
      LOG.info("user_getClientFileInfoByPath(" + filePath + ") : " + ret);
      return ret;
    }
  }

  @Override
  public List<NetAddress> user_getFileLocationsById(int fileId)
      throws FileDoesNotExistException, TException {
    LOG.info("user_getFileLocationsById: " + fileId);
    synchronized (mFiles) {
      INode inode = mFiles.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      LOG.info("user_getFileLocationsById: " + fileId + " good return");
      ArrayList<NetAddress> ret = new ArrayList<NetAddress>(inode.mLocations.size());
      ret.addAll(inode.mLocations.values());
      return ret;
    }
  }

  @Override
  public List<NetAddress> user_getFileLocationsByPath(String filePath)
      throws FileDoesNotExistException, TException {
    LOG.info("user_getFileLocationsByPath(" + filePath + ")");
    synchronized (mFiles) {
      if (!mFilePathToId.containsKey(filePath)) {
        throw new FileDoesNotExistException("File " + filePath + " does not exist.");
      }

      INode inode = mFiles.get(mFilePathToId.get(filePath));
      ArrayList<NetAddress> ret = new ArrayList<NetAddress>(inode.mLocations.size());
      ret.addAll(inode.mLocations.values());
      LOG.info("user_getFileLocationsByPath(" + filePath + ") : " + ret);
      return ret;
    }
  }

  @Override
  public int user_getFileId(String filePath) throws TException {
    LOG.info("user_getFileId(" + filePath + ")");
    int ret = 0;
    synchronized (mFiles) {
      if (mFilePathToId.containsKey(filePath)) {
        ret = mFilePathToId.get(filePath);
      }
    }

    LOG.info("user_getFileId(" + filePath + ") returns FileId " + ret);
    return ret;
  }

  //  @Override
  //  public RawColumnDatasetInfo user_getRawColumnDatasetById(int datasetId)
  //      throws FileDoesNotExistException, TException {
  //    LOG.info("user_getRawColumnDatasetById: " + datasetId);
  //    synchronized (mFiles) {
  //      RawColumnDatasetInfo ret = mRawColumnDatasets.get(datasetId);
  //      if (ret == null) {
  //        throw new FileDoesNotExistException("RawColumnDatasetId " + datasetId +
  //            " does not exist.");
  //      }
  //      LOG.info("user_getRawColumnDatasetById: " + datasetId + " good return");
  //      return ret;
  //    }
  //  }
  //
  //  @Override
  //  public RawColumnDatasetInfo user_getRawColumnDatasetByPath(String datasetPath)
  //      throws FileDoesNotExistException, TException {
  //    LOG.info("user_getRawColumnDatasetByPath(" + datasetPath + ")");
  //    synchronized (mFiles) {
  //      if (!mRawColumnDatasetPathToId.containsKey(datasetPath)) {
  //        throw new FileDoesNotExistException("RawColumnDataset " + datasetPath + 
  //            " does not exist.");
  //      }
  //
  //      RawColumnDatasetInfo ret = mRawColumnDatasets.get(mRawColumnDatasetPathToId.get(datasetPath));
  //      LOG.info("user_getRawColumnDatasetByPath(" + datasetPath + ") : " + ret);
  //      return ret;
  //    }
  //  }
  //
  //  @Override
  //  public int user_getRawColumnDatasetId(String datasetPath) throws TException {
  //    LOG.info("user_getRawColumnDatasetId(" + datasetPath + ")");
  //    int ret = 0;
  //    synchronized (mFiles) {
  //      if (mRawColumnDatasetPathToId.containsKey(datasetPath)) {
  //        ret = mFilePathToId.get(datasetPath);
  //      }
  //    }
  //
  //    LOG.info("user_getRawColumnDatasetId(" + datasetPath + ") returns DatasetId " + ret);
  //    return ret;
  //  }

  @Override
  public long user_getUserId() throws TException {
    return mUserCounter.addAndGet(1);
  }

  @Override
  public void user_outOfMemoryForPinFile(int fileId) throws TException {
    LOG.error("The user can not allocate enough space for PIN list File " + fileId);
  }

  @Override
  public void user_renameFile(String srcDatasetPath, String dstDatasetPath)
      throws FileDoesNotExistException, TException {
    synchronized (mFiles) {
      int fileId = user_getFileId(srcDatasetPath);
      if (fileId <= 0) {
        throw new FileDoesNotExistException("File " + srcDatasetPath + " does not exist");
      }
      mFilePathToId.remove(srcDatasetPath);
      mFilePathToId.put(dstDatasetPath, fileId);
      INode inode = mFiles.get(fileId);
      inode.mName = dstDatasetPath;
      mMasterLogWriter.appendAndFlush(inode);
    }
  }

  //  @Override
  //  public void user_renameRawColumnDataset(String srcDatasetPath, String dstDatasetPath)
  //      throws FileDoesNotExistException, TException {
  //    // TODO Auto-generated method stub
  //    synchronized (mFiles) {
  //      int datasetId = user_getRawColumnDatasetId(srcDatasetPath);
  //      if (datasetId <= 0) {
  //        throw new FileDoesNotExistException("getRawColumnDataset " + srcDatasetPath +
  //            " does not exist");
  //      }
  //      mRawColumnDatasetPathToId.remove(srcDatasetPath);
  //      mRawColumnDatasetPathToId.put(dstDatasetPath, datasetId);
  //      RawColumnDatasetInfo datasetInfo = mRawColumnDatasets.get(datasetId);
  //      datasetInfo.mPath = dstDatasetPath;
  //      for (int k = 0; k < datasetInfo.mColumns; k ++) {
  //        user_renameDataset(srcDatasetPath + "/col_" + k, dstDatasetPath + "/col_" + k);
  //      }
  //
  //      mMasterLogWriter.appendAndFlush(datasetInfo);
  //    }
  //  }

  //  @Override
  //  public void user_setPartitionCheckpointPath(int datasetId, int partitionId,
  //      String checkpointPath) throws FileDoesNotExistException,
  //      FileDoesNotExistException, TException {
  //    synchronized (mFiles) {
  //      if (!mFiles.containsKey(datasetId)) {
  //        throw new FileDoesNotExistException("Dataset " + datasetId + " does not exist");
  //      }
  //
  //      DatasetInfo dataset = mFiles.get(datasetId);
  //
  //      if (partitionId < 0 || partitionId >= dataset.mNumOfPartitions) {
  //        throw new FileDoesNotExistException("Dataset has " + dataset.mNumOfPartitions +
  //            " partitions. However, the request partition id is " + partitionId);
  //      }
  //
  //      dataset.mPartitionList.get(partitionId).mHasCheckpointed = true;
  //      dataset.mPartitionList.get(partitionId).mCheckpointPath = checkpointPath;
  //
  //      mMasterLogWriter.appendAndFlush(dataset.mPartitionList.get(partitionId));
  //    }
  //  }

  @Override
  public void user_unpinFile(int fileId) throws FileDoesNotExistException, TException {
    // TODO Change meta data only. Data will be evicted from worker based on data replacement 
    // policy. TODO May change it to be active from V0.2
    LOG.info("user_unpinFile(" + fileId + ")");
    synchronized (mFiles) {
      if (!mFiles.containsKey(fileId)) {
        throw new FileDoesNotExistException("Failed to unpin " + fileId);
      }

      synchronized (mIdPinList) {
        mIdPinList.remove(fileId);
      }
      INode inode = mFiles.get(fileId);
      inode.mPin = false;
      mMasterLogWriter.appendAndFlush(inode);
    }
  }

  @Override
  public void worker_addFile(long workerId, long workerUsedBytes, int fileId,
      int partitionSizeBytes, boolean hasCheckpointed, String checkpointPath)
          throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    String parameters = CommonUtils.parametersToString(workerId, workerUsedBytes, fileId,
        partitionSizeBytes);
    LOG.info("worker_addFile" + parameters);
    WorkerInfo tWorkerInfo = null;
    synchronized (mWorkers) {
      tWorkerInfo = mWorkers.get(workerId);

      if (tWorkerInfo == null) {
        LOG.error("No worker: " + workerId);
        return;
      }
    }

    tWorkerInfo.updateFile(true, fileId);
    tWorkerInfo.updateUsedBytes(workerUsedBytes);
    tWorkerInfo.updateLastUpdatedTimeMs();

    synchronized (mFiles) {
      INode inode = mFiles.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.mIsFolder) {
        throw new FileDoesNotExistException("File " + fileId + " is a folder.");
      }

      INodeFile tFile = (INodeFile) inode;

      if (tFile.getLength() != INode.UNINITIAL_VALUE) {
        if (tFile.getLength() != partitionSizeBytes) {
          throw new SuspectedFileSizeException(fileId + ". Original Size: " +
              tFile.getLength() + ". New Size: " + partitionSizeBytes);
        }
      } else {
        tFile.setLength(partitionSizeBytes);
      }
      if (hasCheckpointed) {
        tFile.mHasCheckpointed = true;
        tFile.mCheckpointPath = checkpointPath;
      }
      InetSocketAddress address = tWorkerInfo.ADDRESS;
      tFile.mLocations.put(workerId, new NetAddress(address.getHostName(), address.getPort()));
    }
  }

  //  @Override
  //  public void worker_addRCDPartition(long workerId, int datasetId, int partitionId,
  //      int partitionSizeBytes) throws FileDoesNotExistException,
  //      SuspectedFileSizeException, TException {
  //    String parameters = CommonUtils.parametersToString(workerId, datasetId, partitionId,
  //        partitionSizeBytes);
  //    LOG.info("worker_addRCDPartition" + parameters);
  //    WorkerInfo tWorkerInfo = null;
  //    synchronized (mWorkers) {
  //      tWorkerInfo = mWorkers.get(workerId);
  //
  //      if (tWorkerInfo == null) {
  //        LOG.error("No worker: " + workerId);
  //        return;
  //      }
  //    }
  //
  //    tWorkerInfo.updateFile(true, CommonUtils.generateBigId(datasetId, partitionId));
  //    tWorkerInfo.updateLastUpdatedTimeMs();
  //
  //    synchronized (mFiles) {
  //      RawColumnDatasetInfo datasetInfo = mRawColumnDatasets.get(datasetId);
  //
  //      if (partitionId < 0 || partitionId >= datasetInfo.mNumOfPartitions) {
  //        throw new FileDoesNotExistException("RawColumnDatasetId " + datasetId + " has " +
  //            datasetInfo.mNumOfPartitions + " partitions. The requested partition id " + 
  //            partitionId + " is out of index.");
  //      }
  //
  //      PartitionInfo pInfo = datasetInfo.mPartitionList.get(partitionId);
  //      if (pInfo.mSizeBytes != -1) {
  //        if (pInfo.mSizeBytes != partitionSizeBytes) {
  //          throw new SuspectedFileSizeException(datasetId + "-" + partitionId + 
  //              ". Original Size: " + pInfo.mSizeBytes + ". New Size: " + partitionSizeBytes);
  //        }
  //      } else {
  //        pInfo.mSizeBytes = partitionSizeBytes;
  //        datasetInfo.mSizeBytes += pInfo.mSizeBytes;
  //      }
  //      InetSocketAddress address = tWorkerInfo.ADDRESS;
  //      pInfo.mLocations.put(workerId, new NetAddress(address.getHostName(), address.getPort()));
  //    }
  //  }

  @Override
  public Set<Integer> worker_getPinList() throws TException {
    Set<Integer> ret = new HashSet<Integer>();
    synchronized (mIdPinList) {
      for (int id : mIdPinList) {
        ret.add(id);
      }
    }
    return ret;
  }

  @Override
  public Command worker_heartbeat(long workerId, long usedBytes, List<Integer> removedFiles)
      throws TException {
    LOG.info("worker_heartbeat(): WorkerId: " + workerId);
    synchronized (mWorkers) {
      if (!mWorkers.containsKey(workerId)) {
        LOG.info("worker_heartbeat(): Does not contain worker with ID " + workerId +
            " . Send command to let it re-register.");
        return new Command(CommandType.Register, ByteBuffer.allocate(0));
      } else {
        WorkerInfo tWorkerInfo = mWorkers.get(workerId);
        tWorkerInfo.updateUsedBytes(usedBytes);
        tWorkerInfo.updateFiles(false, removedFiles);
        tWorkerInfo.updateLastUpdatedTimeMs();

        synchronized (mFiles) {
          for (int fileId : removedFiles) {
            INode inode = mFiles.get(fileId);
            if (inode == null) {
              LOG.error("Data " + fileId + " does not exist");
            } else {
              inode.mLocations.remove(workerId);
            }
          }
        }
      }
    }

    return new Command(CommandType.Nothing, ByteBuffer.allocate(0));
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, long totalBytes, long usedBytes,
      List<Integer> currentFiles) throws TException {
    long id = 0;
    InetSocketAddress workerAddress =
        new InetSocketAddress(workerNetAddress.mHost, workerNetAddress.mPort);
    LOG.info("worker_register(): WorkerNetAddress: " + workerAddress);

    synchronized (mWorkers) {
      if (mWorkerAddressToId.containsKey(workerAddress)) {
        id = mWorkerAddressToId.get(workerAddress);
        mWorkerAddressToId.remove(id);
        LOG.warn("The worker " + workerAddress + " already exists as id " + id + ".");
      }
      if (id != 0 && mWorkers.containsKey(id)) {
        WorkerInfo tWorkerInfo = mWorkers.get(id);
        mWorkers.remove(id);
        mLostWorkers.add(tWorkerInfo);
        LOG.warn("The worker with id " + id + " has been removed.");
      }
      id = START_TIME_NS_PREFIX + mWorkerCounter.addAndGet(1);
      WorkerInfo tWorkerInfo = new WorkerInfo(id, workerAddress, totalBytes);
      tWorkerInfo.updateUsedBytes(usedBytes);
      tWorkerInfo.updateFiles(true, currentFiles);
      tWorkerInfo.updateLastUpdatedTimeMs();
      mWorkers.put(id, tWorkerInfo);
      mWorkerAddressToId.put(workerAddress, id);
      LOG.info("worker_register(): " + tWorkerInfo);
    }

    for (long fileId: currentFiles) {
      synchronized (mFiles) {
        INode inode = mFiles.get(fileId);
        if (inode != null) {
          inode.mLocations.put(id, workerNetAddress);
        } else {
          LOG.warn("worker_register failed to add fileId " + fileId);
        }
      }
    }

    return id;
  }

  private void writeCheckpoint() {
    LOG.info("Files recoveried from logs: ");
    synchronized (mFiles) {
      MasterLogWriter checkpointWriter =
          new MasterLogWriter(Config.MASTER_CHECKPOINT_FILE + ".tmp");
      int maxDatasetId = 0;
      for (INode inode : mFiles.values()) {
        checkpointWriter.appendAndFlush(inode);
        LOG.info(inode.toString());
        maxDatasetId = Math.max(maxDatasetId, inode.mId);
        if (mPinList.inList(inode.mName)) {
          mIdPinList.add(inode.mId);
        }
      }
      //      for (RawColumnDatasetInfo dataset : mRawColumnDatasets.values()) {
      //        checkpointWriter.appendAndFlush(dataset);
      //        LOG.info(dataset.toString());
      //        maxDatasetId = Math.max(maxDatasetId, dataset.mId);
      //      }
      //      if (maxDatasetId != mDatasetCounter.get() && mDatasetCounter.get() != 0) {
      //        INode tempDataset = new DatasetInfo();
      //        tempDataset.mId = - mDatasetCounter.get();
      //        checkpointWriter.appendAndFlush(tempDataset);
      //      }
      checkpointWriter.close();

      File srcFile = new File(Config.MASTER_CHECKPOINT_FILE + ".tmp");
      File dstFile = new File(Config.MASTER_CHECKPOINT_FILE);
      if (!srcFile.renameTo(dstFile)) {
        CommonUtils.runtimeException("Failed to rename file from " + srcFile.getPath() +
            " to " + dstFile.getPath());
      }

      File file = new File(Config.MASTER_LOG_FILE);
      if (file.exists()) {
        while (!file.delete()) {
          LOG.info("Trying to delete " + file.toString());
          CommonUtils.sleep(1000);
        }
      }
    }
    LOG.info("Files recovery done. Current mDatasetCounter: " + mDatasetCounter.get());
  }

  private void recoveryFromFile(String fileName, String msg) {
    MasterLogReader reader;

    File file = new File(fileName);
    if (!file.exists()) {
      LOG.info(msg + fileName + " does not exist.");
    } else {
      reader = new MasterLogReader(fileName);
      while (reader.hasNext()) {
        Pair<LogEventType, Object> pair = reader.getNextDatasetInfo();
        switch (pair.getFirst()) {
          case INode: {
            INode inode = (INode) pair.getSecond();
            if (Math.abs(inode.mId) > mDatasetCounter.get()) {
              mDatasetCounter.set(Math.abs(inode.mId));
            }

            System.out.println("Putting " + inode);
            if (inode.mId > 0) {
              mFiles.put(inode.mId, inode);
              mFilePathToId.put(inode.mName, inode.mId);
            } else {
              mFiles.remove(- inode.mId);
              mFilePathToId.remove(inode.mName);
            }
            break;
          }
          //          case RawColumnDatasetInfo: {
          //            RawColumnDatasetInfo dataset = (RawColumnDatasetInfo) pair.getSecond();
          //            if (Math.abs(dataset.mId) > mDatasetCounter.get()) {
          //              mDatasetCounter.set(Math.abs(dataset.mId));
          //            }
          //
          //            System.out.println("Putting " + dataset);
          //            if (dataset.mId > 0) {
          //              mRawColumnDatasets.put(dataset.mId, dataset);
          //              mRawColumnDatasetPathToId.put(dataset.mPath, dataset.mId);
          //            } else {
          //              mRawColumnDatasets.remove(- dataset.mId);
          //              mRawColumnDatasetPathToId.remove(dataset.mPath);
          //            }
          //            break;
          //          }
          case Undefined:
            CommonUtils.runtimeException("Corruptted info from " + fileName + 
                ". It has undefined data type.");
            break;
          default:
            CommonUtils.runtimeException("Corruptted info from " + fileName);
        }
      }
    }
  }

  private void recoveryFromLog() {
    synchronized (mFiles) {
      recoveryFromFile(Config.MASTER_CHECKPOINT_FILE, "Master Checkpoint file ");
      recoveryFromFile(Config.MASTER_LOG_FILE, "Master Log file ");
    }
  }
}