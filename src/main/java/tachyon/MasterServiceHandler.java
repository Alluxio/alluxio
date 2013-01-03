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

import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.LogEventType;
import tachyon.thrift.MasterService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.PartitionDoesNotExistException;
import tachyon.thrift.PartitionInfo;
import tachyon.thrift.DatasetAlreadyExistException;
import tachyon.thrift.DatasetDoesNotExistException;
import tachyon.thrift.DatasetInfo;
import tachyon.thrift.RawColumnDatasetInfo;
import tachyon.thrift.SuspectedPartitionSizeException;

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
  private Map<Integer, DatasetInfo> mDatasets = new HashMap<Integer, DatasetInfo>();
  private Map<String, Integer> mDatasetPathToId = new HashMap<String, Integer>();

  private Map<Integer, RawColumnDatasetInfo> mRawColumnDatasets = 
      new HashMap<Integer, RawColumnDatasetInfo>();
  private Map<String, Integer> mRawColumnDatasetPathToId = new HashMap<String, Integer>();

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

        for (long id: worker.getPartitions()) {
          int datasetId = CommonUtils.computeDatasetIdFromBigId(id);
          int pId = CommonUtils.computePartitionIdFromBigId(id);

          synchronized (mDatasets) {
            DatasetInfo tDatasetInfo = mDatasets.get(datasetId);
            if (tDatasetInfo != null) {
              PartitionInfo pInfo = tDatasetInfo.mPartitionList.get(pId);
              Map<Long, NetAddress> locations = pInfo.mLocations;

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
  public List<DatasetInfo> cmd_ls(String folder) throws TException {
    ArrayList<DatasetInfo> ret = new ArrayList<DatasetInfo>();
    synchronized (mDatasets) {
      for (DatasetInfo datasetInfo : mDatasets.values()) {
        if (datasetInfo.mPath.startsWith(folder)) {
          ret.add(datasetInfo);
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
      synchronized (mDatasets) {
        sb.append("<h2>" + mWorkers.size() + " worker(s) are running: </h2>");
        List<Long> workerList = new ArrayList<Long>(mWorkers.keySet());
        Collections.sort(workerList);
        for (int k = 0; k < workerList.size(); k ++) {
          sb.append("<strong>Worker " + (k + 1) + " </strong>: " + 
              mWorkers.get(workerList.get(k)).toHtml() + "<br \\>");
        }

        sb.append("<h2>" + mDatasets.size() + " File(s): </h2>");
        List<Integer> datasetList = new ArrayList<Integer>(mDatasets.keySet());
        Collections.sort(datasetList);
        for (int k = 0; k < datasetList.size(); k ++) {
          DatasetInfo tDataset = mDatasets.get(datasetList.get(k));
          sb.append("<strong>File " + (k + 1) + " </strong>: ");
          sb.append("ID: ").append(tDataset.mId).append("; ");
          sb.append("Path: ").append(tDataset.mPath).append("; ");
          sb.append("NumberOfPartitions: ").append(tDataset.getMPartitionListSize()).append("; ");
          if (Config.DEBUG) {
            sb.append(tDataset.toString());
          }
          sb.append("<br \\>");
        }
      }
    }

    return sb.toString();
  }

  @Override
  public int user_createDataset(String datasetPath, int partitions)
      throws DatasetAlreadyExistException, TException, InvalidPathException {
    return user_createDataset(datasetPath, partitions, false, -1);
  }

  public int user_createDataset(String datasetPath, int partitions, boolean isSubDataset,
      int parentDatasetId) throws DatasetAlreadyExistException, TException, InvalidPathException {
    String parameters = CommonUtils.parametersToString(datasetPath, partitions);
    LOG.info("user_createDataset" + parameters);

    DatasetInfo dataset = null;

    synchronized (mDatasets) {
      if (mDatasetPathToId.containsKey(datasetPath)) {
        LOG.info(datasetPath + " already exists.");
        throw new DatasetAlreadyExistException("Dataset " + datasetPath + " already exists.");
      }

      dataset = new DatasetInfo();
      dataset.mId = mDatasetCounter.addAndGet(1);
      dataset.mPath = datasetPath;
      dataset.mSizeBytes = 0;
      dataset.mNumOfPartitions = partitions;
      dataset.mPartitionList = new ArrayList<PartitionInfo>(partitions);
      for (int k = 0; k < partitions; k ++) {
        PartitionInfo partition = new PartitionInfo();
        partition.mDatasetId = dataset.mId;
        partition.mPartitionId = k;
        partition.mSizeBytes = -1;
        partition.mLocations = new HashMap<Long, NetAddress>();
        dataset.mPartitionList.add(partition);
      }
      dataset.setMCache(mWhiteList.inList(dataset.mPath));
      dataset.setMPin(mPinList.inList(dataset.mPath));
      dataset.mIsSubDataset = isSubDataset;
      dataset.mParentDatasetId = parentDatasetId;

      mMasterLogWriter.appendAndFlush(dataset);

      mDatasetPathToId.put(datasetPath, dataset.mId);
      mDatasets.put(dataset.mId, dataset);

      if (mPinList.inList(datasetPath)) {
        mIdPinList.add(dataset.mId);
      }

      LOG.info("user_createDataset: Dataset Created: " + dataset);
    }

    return dataset.mId;
  }

  @Override
  public int user_createRawColumnDataset(String datasetPath, int columns, int partitions)
      throws DatasetAlreadyExistException, InvalidPathException, TException {
    String parameters = CommonUtils.parametersToString(datasetPath, columns, partitions);
    LOG.info("user_createRawColumnDataset" + parameters);

    RawColumnDatasetInfo rawColumnDataset = null;

    synchronized (mDatasets) {
      if (mDatasetPathToId.containsKey(datasetPath) 
          || mRawColumnDatasetPathToId.containsKey(datasetPath)) {
        LOG.info(datasetPath + " already exists.");
        throw new DatasetAlreadyExistException("RawColumnDataset " + datasetPath + 
            " already exists.");
      }

      rawColumnDataset = new RawColumnDatasetInfo();
      rawColumnDataset.mId = mDatasetCounter.addAndGet(1);
      rawColumnDataset.mPath = datasetPath;
      rawColumnDataset.mColumns = columns;
      rawColumnDataset.mNumOfPartitions = partitions;
      rawColumnDataset.mColumnDatasetIdList = new ArrayList<Integer>(columns);
      for (int k = 0; k < columns; k ++) {
        rawColumnDataset.mColumnDatasetIdList.add(
            user_createDataset(datasetPath + "/col_" + k, partitions, true, rawColumnDataset.mId));
      }
      rawColumnDataset.mPartitionList = new ArrayList<PartitionInfo>(partitions);
      for (int k = 0; k < partitions; k ++) {
        PartitionInfo partition = new PartitionInfo();
        partition.mDatasetId = rawColumnDataset.mId;
        partition.mPartitionId = k;
        partition.mSizeBytes = -1;
        partition.mLocations = new HashMap<Long, NetAddress>();
        rawColumnDataset.mPartitionList.add(partition);
      }

      mMasterLogWriter.appendAndFlush(rawColumnDataset);

      mRawColumnDatasetPathToId.put(datasetPath, rawColumnDataset.mId);
      mRawColumnDatasets.put(rawColumnDataset.mId, rawColumnDataset);

      LOG.info("user_createRawColumnDataset: RawColumnDataset Created: " + rawColumnDataset);
    }

    return rawColumnDataset.mId;
  }

  @Override
  public void user_deleteDataset(int datasetId) throws DatasetDoesNotExistException, TException {
    LOG.info("user_deleteDataset(" + datasetId + ")");
    // Only remove meta data from master. The data in workers will be evicted since no further
    // application can read them. (Based on LRU) TODO May change it to be active from V0.2. 
    synchronized (mDatasets) {
      if (!mDatasets.containsKey(datasetId)) {
        throw new DatasetDoesNotExistException("Failed to delete " + datasetId + " dataset.");
      }

      synchronized (mIdPinList) {
        mIdPinList.remove(new Integer(datasetId));
      }
      DatasetInfo dataset = mDatasets.remove(datasetId);
      mDatasetPathToId.remove(dataset.mPath);
      dataset.mId = - dataset.mId;

      mMasterLogWriter.appendAndFlush(dataset);
    }
  }

  @Override
  public void user_deleteRawColumnDataset(int datasetId)
      throws DatasetDoesNotExistException, TException {
    // TODO Auto-generated method stub
    LOG.info("user_deleteDataset(" + datasetId + ")");
    // Only remove meta data from master. The data in workers will be evicted since no further
    // application can read them. (Based on LRU) TODO May change it to be active from V0.2. 
    synchronized (mDatasets) {
      if (!mRawColumnDatasets.containsKey(datasetId)) {
        throw new DatasetDoesNotExistException("Failed to delete " + datasetId + 
            " RawColumnDataset.");
      }

      RawColumnDatasetInfo dataset = mRawColumnDatasets.remove(datasetId);
      mRawColumnDatasetPathToId.remove(dataset.mPath);
      dataset.mId = - dataset.mId;

      for (int k = 0; k < dataset.mColumns; k ++) {
        user_deleteDataset(dataset.mColumnDatasetIdList.get(k));
      }

      // TODO this order is not right. Move this upper.
      mMasterLogWriter.appendAndFlush(dataset);
    }

  }

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
  public PartitionInfo user_getPartitionInfo(int datasetId, int partitionId)
      throws PartitionDoesNotExistException, TException {
    PartitionInfo ret;
    LOG.info("user_getPartitionInfo( " + datasetId + "," + partitionId + ")");
    synchronized (mDatasets) {
      DatasetInfo tDatasetInfo = mDatasets.get(datasetId);
      if (tDatasetInfo == null) {
        throw new PartitionDoesNotExistException("DatasetId " + datasetId + " does not exist.");
      }
      if (partitionId < 0 || partitionId >= tDatasetInfo.mNumOfPartitions) {
        throw new PartitionDoesNotExistException("DatasetId " + datasetId + " has " +
            tDatasetInfo.mNumOfPartitions + " partitions. The requested partition id " + 
            partitionId + " is out of index.");
      }
      ret = tDatasetInfo.mPartitionList.get(partitionId);
    }
    LOG.info("user_getPartitionInfo() returns partition info: " + ret);
    return ret;
  }

  @Override
  public DatasetInfo user_getDatasetById(int datasetId) 
      throws DatasetDoesNotExistException, TException {
    LOG.info("user_getDatasetById: " + datasetId);
    synchronized (mDatasets) {
      DatasetInfo ret = mDatasets.get(datasetId);
      if (ret == null) {
        throw new DatasetDoesNotExistException("DatasetId " + datasetId + " does not exist.");
      }
      LOG.info("user_getDatasetById: " + datasetId + " good return");
      return ret;
    }
  }

  @Override
  public DatasetInfo user_getDatasetByPath(String datasetPath)
      throws DatasetDoesNotExistException, TException {
    LOG.info("user_getDatasetByPath(" + datasetPath + ")");
    synchronized (mDatasets) {
      if (!mDatasetPathToId.containsKey(datasetPath)) {
        throw new DatasetDoesNotExistException("Dataset " + datasetPath + " does not exist.");
      }

      DatasetInfo ret = mDatasets.get(mDatasetPathToId.get(datasetPath));
      LOG.info("user_getDatasetByPath(" + datasetPath + ") : " + ret);
      return ret;
    }
  }

  @Override
  public int user_getDatasetId(String datasetPath) throws TException {
    LOG.info("user_getDatasetId(" + datasetPath + ")");
    int ret = 0;
    synchronized (mDatasets) {
      if (mDatasetPathToId.containsKey(datasetPath)) {
        ret = mDatasetPathToId.get(datasetPath);
      }
    }

    LOG.info("user_getDatasetId(" + datasetPath + ") returns DatasetId " + ret);
    return ret;
  }

  @Override
  public RawColumnDatasetInfo user_getRawColumnDatasetById(int datasetId)
      throws DatasetDoesNotExistException, TException {
    LOG.info("user_getRawColumnDatasetById: " + datasetId);
    synchronized (mDatasets) {
      RawColumnDatasetInfo ret = mRawColumnDatasets.get(datasetId);
      if (ret == null) {
        throw new DatasetDoesNotExistException("RawColumnDatasetId " + datasetId +
            " does not exist.");
      }
      LOG.info("user_getRawColumnDatasetById: " + datasetId + " good return");
      return ret;
    }
  }

  @Override
  public RawColumnDatasetInfo user_getRawColumnDatasetByPath(String datasetPath)
      throws DatasetDoesNotExistException, TException {
    LOG.info("user_getRawColumnDatasetByPath(" + datasetPath + ")");
    synchronized (mDatasets) {
      if (!mRawColumnDatasetPathToId.containsKey(datasetPath)) {
        throw new DatasetDoesNotExistException("RawColumnDataset " + datasetPath + 
            " does not exist.");
      }

      RawColumnDatasetInfo ret = mRawColumnDatasets.get(mRawColumnDatasetPathToId.get(datasetPath));
      LOG.info("user_getRawColumnDatasetByPath(" + datasetPath + ") : " + ret);
      return ret;
    }
  }

  @Override
  public int user_getRawColumnDatasetId(String datasetPath) throws TException {
    LOG.info("user_getRawColumnDatasetId(" + datasetPath + ")");
    int ret = 0;
    synchronized (mDatasets) {
      if (mRawColumnDatasetPathToId.containsKey(datasetPath)) {
        ret = mDatasetPathToId.get(datasetPath);
      }
    }

    LOG.info("user_getRawColumnDatasetId(" + datasetPath + ") returns DatasetId " + ret);
    return ret;
  }

  @Override
  public long user_getUserId() throws TException {
    return mUserCounter.addAndGet(1);
  }

  @Override
  public void user_outOfMemoryForPinDataset(int datasetId) throws TException {
    LOG.error("The user can not allocate enough space for PIN list Dataset " + datasetId);
  }

  @Override
  public void user_renameDataset(String srcDatasetPath, String dstDatasetPath)
      throws DatasetDoesNotExistException, TException {
    synchronized (mDatasets) {
      int datasetId = user_getDatasetId(srcDatasetPath);
      if (datasetId <= 0) {
        throw new DatasetDoesNotExistException("Dataset " + srcDatasetPath + " does not exist");
      }
      mDatasetPathToId.remove(srcDatasetPath);
      mDatasetPathToId.put(dstDatasetPath, datasetId);
      DatasetInfo datasetInfo = mDatasets.get(datasetId);
      datasetInfo.mPath = dstDatasetPath;
      mMasterLogWriter.appendAndFlush(datasetInfo);
    }
  }

  @Override
  public void user_renameRawColumnDataset(String srcDatasetPath, String dstDatasetPath)
      throws DatasetDoesNotExistException, TException {
    // TODO Auto-generated method stub
    synchronized (mDatasets) {
      int datasetId = user_getRawColumnDatasetId(srcDatasetPath);
      if (datasetId <= 0) {
        throw new DatasetDoesNotExistException("getRawColumnDataset " + srcDatasetPath +
            " does not exist");
      }
      mRawColumnDatasetPathToId.remove(srcDatasetPath);
      mRawColumnDatasetPathToId.put(dstDatasetPath, datasetId);
      RawColumnDatasetInfo datasetInfo = mRawColumnDatasets.get(datasetId);
      datasetInfo.mPath = dstDatasetPath;
      for (int k = 0; k < datasetInfo.mColumns; k ++) {
        user_renameDataset(srcDatasetPath + "/col_" + k, dstDatasetPath + "/col_" + k);
      }

      mMasterLogWriter.appendAndFlush(datasetInfo);
    }
  }

  @Override
  public void user_setPartitionCheckpointPath(int datasetId, int partitionId,
      String checkpointPath) throws DatasetDoesNotExistException,
      PartitionDoesNotExistException, TException {
    synchronized (mDatasets) {
      if (!mDatasets.containsKey(datasetId)) {
        throw new DatasetDoesNotExistException("Dataset " + datasetId + " does not exist");
      }

      DatasetInfo dataset = mDatasets.get(datasetId);

      if (partitionId < 0 || partitionId >= dataset.mNumOfPartitions) {
        throw new PartitionDoesNotExistException("Dataset has " + dataset.mNumOfPartitions +
            " partitions. However, the request partition id is " + partitionId);
      }

      dataset.mPartitionList.get(partitionId).mHasCheckpointed = true;
      dataset.mPartitionList.get(partitionId).mCheckpointPath = checkpointPath;

      mMasterLogWriter.appendAndFlush(dataset.mPartitionList.get(partitionId));
    }
  }

  @Override
  public void user_unpinDataset(int datasetId) throws DatasetDoesNotExistException, TException {
    // TODO Change meta data only. Data will be evicted from worker based on data replacement 
    // policy. TODO May change it to be active from V0.2
    LOG.info("user_freeDataset(" + datasetId + ")");
    synchronized (mDatasets) {
      if (!mDatasets.containsKey(datasetId)) {
        throw new DatasetDoesNotExistException("Failed to free " + datasetId + " dataset.");
      }

      synchronized (mIdPinList) {
        mIdPinList.remove(new Integer(datasetId));
      }
      DatasetInfo dataset = mDatasets.get(datasetId);
      dataset.setMPin(false);
      mMasterLogWriter.appendAndFlush(dataset);
    }
  }

  @Override
  public void worker_addPartition(long workerId, long workerUsedBytes, int datasetId,
      int partitionId, int partitionSizeBytes, boolean hasCheckpointed, String checkpointPath)
          throws PartitionDoesNotExistException, SuspectedPartitionSizeException, TException {
    String parameters = CommonUtils.parametersToString(workerId, workerUsedBytes, datasetId,
        partitionId, partitionSizeBytes);
    LOG.info("worker_addPartition" + parameters);
    WorkerInfo tWorkerInfo = null;
    synchronized (mWorkers) {
      tWorkerInfo = mWorkers.get(workerId);

      if (tWorkerInfo == null) {
        LOG.error("No worker: " + workerId);
        return;
      }
    }

    tWorkerInfo.updatePartition(true, CommonUtils.generateBigId(datasetId, partitionId));
    tWorkerInfo.updateUsedBytes(workerUsedBytes);
    tWorkerInfo.updateLastUpdatedTimeMs();

    synchronized (mDatasets) {
      DatasetInfo datasetInfo = mDatasets.get(datasetId);

      if (partitionId < 0 || partitionId >= datasetInfo.mNumOfPartitions) {
        throw new PartitionDoesNotExistException("DatasetId " + datasetId + " has " +
            datasetInfo.mNumOfPartitions + " partitions. The requested partition id " + 
            partitionId + " is out of index.");
      }

      PartitionInfo pInfo = datasetInfo.mPartitionList.get(partitionId);
      if (pInfo.mSizeBytes != -1) {
        if (pInfo.mSizeBytes != partitionSizeBytes) {
          throw new SuspectedPartitionSizeException(datasetId + "-" + partitionId + 
              ". Original Size: " + pInfo.mSizeBytes + ". New Size: " + partitionSizeBytes);
        }
      } else {
        pInfo.mSizeBytes = partitionSizeBytes;
        datasetInfo.mSizeBytes += pInfo.mSizeBytes;
      }
      if (hasCheckpointed) {
        pInfo.mHasCheckpointed = true;
        pInfo.mCheckpointPath = checkpointPath;
      }
      InetSocketAddress address = tWorkerInfo.ADDRESS;
      pInfo.mLocations.put(workerId, new NetAddress(address.getHostName(), address.getPort()));
    }
  }

  @Override
  public void worker_addRCDPartition(long workerId, int datasetId, int partitionId,
      int partitionSizeBytes) throws PartitionDoesNotExistException,
      SuspectedPartitionSizeException, TException {
    String parameters = CommonUtils.parametersToString(workerId, datasetId, partitionId,
        partitionSizeBytes);
    LOG.info("worker_addRCDPartition" + parameters);
    WorkerInfo tWorkerInfo = null;
    synchronized (mWorkers) {
      tWorkerInfo = mWorkers.get(workerId);

      if (tWorkerInfo == null) {
        LOG.error("No worker: " + workerId);
        return;
      }
    }

    tWorkerInfo.updatePartition(true, CommonUtils.generateBigId(datasetId, partitionId));
    tWorkerInfo.updateLastUpdatedTimeMs();

    synchronized (mDatasets) {
      RawColumnDatasetInfo datasetInfo = mRawColumnDatasets.get(datasetId);

      if (partitionId < 0 || partitionId >= datasetInfo.mNumOfPartitions) {
        throw new PartitionDoesNotExistException("RawColumnDatasetId " + datasetId + " has " +
            datasetInfo.mNumOfPartitions + " partitions. The requested partition id " + 
            partitionId + " is out of index.");
      }

      PartitionInfo pInfo = datasetInfo.mPartitionList.get(partitionId);
      if (pInfo.mSizeBytes != -1) {
        if (pInfo.mSizeBytes != partitionSizeBytes) {
          throw new SuspectedPartitionSizeException(datasetId + "-" + partitionId + 
              ". Original Size: " + pInfo.mSizeBytes + ". New Size: " + partitionSizeBytes);
        }
      } else {
        pInfo.mSizeBytes = partitionSizeBytes;
        datasetInfo.mSizeBytes += pInfo.mSizeBytes;
      }
      InetSocketAddress address = tWorkerInfo.ADDRESS;
      pInfo.mLocations.put(workerId, new NetAddress(address.getHostName(), address.getPort()));
    }
  }

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
  public Command worker_heartbeat(long workerId, long usedBytes,
      List<Long> removedPartitionList) throws TException {
    LOG.info("worker_heartbeat(): WorkerId: " + workerId);
    synchronized (mWorkers) {
      if (!mWorkers.containsKey(workerId)) {
        LOG.info("worker_heartbeat(): Does not contain worker with ID " + workerId +
            " . Send command to let it re-register.");
        return new Command(CommandType.Register, ByteBuffer.allocate(0));
      } else {
        WorkerInfo tWorkerInfo = mWorkers.get(workerId);
        tWorkerInfo.updateUsedBytes(usedBytes);
        tWorkerInfo.updatePartitions(false, removedPartitionList);
        tWorkerInfo.updateLastUpdatedTimeMs();

        synchronized (mDatasets) {
          for (long bigId : removedPartitionList) {
            int datasetId = CommonUtils.computeDatasetIdFromBigId(bigId);
            int pId = CommonUtils.computePartitionIdFromBigId(bigId);
            DatasetInfo datasetInfo = mDatasets.get(datasetId);
            if (datasetInfo == null) {
              LOG.error("Data " + datasetId + " does not exist");
            } else {
              PartitionInfo pInfo = datasetInfo.mPartitionList.get(pId);
              pInfo.mLocations.remove(workerId);
            }
          }
        }
      }
    }

    return new Command(CommandType.Nothing, ByteBuffer.allocate(0));
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Long> currentPartitionList) throws TException {
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
      tWorkerInfo.updatePartitions(true, currentPartitionList);
      tWorkerInfo.updateLastUpdatedTimeMs();
      mWorkers.put(id, tWorkerInfo);
      mWorkerAddressToId.put(workerAddress, id);
      LOG.info("worker_register(): " + tWorkerInfo);
    }

    for (long wId: currentPartitionList) {
      int datasetId = CommonUtils.computeDatasetIdFromBigId(wId);
      int pId = CommonUtils.computePartitionIdFromBigId(wId);

      synchronized (mDatasets) {
        DatasetInfo tDatasetInfo = mDatasets.get(datasetId);
        if (tDatasetInfo != null) {
          PartitionInfo pInfo = tDatasetInfo.mPartitionList.get(pId);
          pInfo.mLocations.put(id, workerNetAddress);
        } else {
          LOG.warn("worker_register failed to add datasetId " + datasetId + " partitionId " + pId);
        }
      }
    }

    return id;
  }

  private void writeCheckpoint() {
    LOG.info("Datasets recoveried from logs: ");
    synchronized (mDatasets) {
      MasterLogWriter checkpointWriter =
          new MasterLogWriter(Config.MASTER_CHECKPOINT_FILE + ".tmp");
      int maxDatasetId = 0;
      for (DatasetInfo dataset : mDatasets.values()) {
        checkpointWriter.appendAndFlush(dataset);
        LOG.info(dataset.toString());
        maxDatasetId = Math.max(maxDatasetId, dataset.mId);
        if (mPinList.inList(dataset.mPath)) {
          mIdPinList.add(dataset.mId);
        }
      }
      for (RawColumnDatasetInfo dataset : mRawColumnDatasets.values()) {
        checkpointWriter.appendAndFlush(dataset);
        LOG.info(dataset.toString());
        maxDatasetId = Math.max(maxDatasetId, dataset.mId);
      }
      if (maxDatasetId != mDatasetCounter.get() && mDatasetCounter.get() != 0) {
        DatasetInfo tempDataset = new DatasetInfo();
        tempDataset.mId = - mDatasetCounter.get();
        checkpointWriter.appendAndFlush(tempDataset);
      }
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
    LOG.info("Datasets recoveried done. Current mDatasetCounter: " + mDatasetCounter.get());
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
          case PartitionInfo: {
            PartitionInfo partition = (PartitionInfo) pair.getSecond();
            System.out.println("Putting " + partition);
            if (mDatasets.containsKey(partition.mDatasetId)) {
              partition.mLocations.clear();
              mDatasets.get(partition.mDatasetId).mPartitionList.set(
                  partition.mPartitionId, partition);
            } else {
              CommonUtils.illegalArgumentException("Corrupted log.");
            }
          }
          case DatasetInfo: {
            DatasetInfo dataset = (DatasetInfo) pair.getSecond();
            if (Math.abs(dataset.mId) > mDatasetCounter.get()) {
              mDatasetCounter.set(Math.abs(dataset.mId));
            }

            System.out.println("Putting " + dataset);
            if (dataset.mId > 0) {
              mDatasets.put(dataset.mId, dataset);
              mDatasetPathToId.put(dataset.mPath, dataset.mId);
            } else {
              mDatasets.remove(- dataset.mId);
              mDatasetPathToId.remove(dataset.mPath);
            }
            break;
          }
          case RawColumnDatasetInfo: {
            RawColumnDatasetInfo dataset = (RawColumnDatasetInfo) pair.getSecond();
            if (Math.abs(dataset.mId) > mDatasetCounter.get()) {
              mDatasetCounter.set(Math.abs(dataset.mId));
            }

            System.out.println("Putting " + dataset);
            if (dataset.mId > 0) {
              mRawColumnDatasets.put(dataset.mId, dataset);
              mRawColumnDatasetPathToId.put(dataset.mPath, dataset.mId);
            } else {
              mRawColumnDatasets.remove(- dataset.mId);
              mRawColumnDatasetPathToId.remove(dataset.mPath);
            }
            break;
          }
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
    synchronized (mDatasets) {
      recoveryFromFile(Config.MASTER_CHECKPOINT_FILE, "Master Checkpoint file ");
      recoveryFromFile(Config.MASTER_LOG_FILE, "Master Log file ");
    }
  }
}