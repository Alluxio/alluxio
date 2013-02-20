package tachyon;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.Command;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
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

  private final MasterInfo mMasterInfo;

  //  private Map<Integer, RawColumnDatasetInfo> mRawColumnDatasets = 
  //      new HashMap<Integer, RawColumnDatasetInfo>();
  //  private Map<String, Integer> mRawColumnDatasetPathToId = new HashMap<String, Integer>();

  // Fault Recovery Log

  public MasterServiceHandler(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  @Override
  public List<String> cmd_ls(String path)
      throws InvalidPathException, FileDoesNotExistException, TException {
    return mMasterInfo.ls(path);
  }

  @Override
  public int user_createFile(String filePath)
      throws FileAlreadyExistException, InvalidPathException, TException {
    return mMasterInfo.createFile(filePath);
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
  public void user_deleteById(int id) throws TException {
    mMasterInfo.delete(id);
  }

  @Override
  public void user_deleteByPath(String path)
      throws InvalidPathException, FileDoesNotExistException, TException {
    mMasterInfo.delete(path);
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
  public NetAddress user_getLocalWorker(String host) throws NoLocalWorkerException, TException {
    return mMasterInfo.getLocalWorker(host);
  }

  @Override
  public ClientFileInfo user_getClientFileInfoById(int id)
      throws FileDoesNotExistException, TException {
    return mMasterInfo.getClientFileInfo(id);
  }

  @Override
  public ClientFileInfo user_getClientFileInfoByPath(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.getClientFileInfo(path);
  }

  @Override
  public List<NetAddress> user_getFileLocationsById(int fileId)
      throws FileDoesNotExistException, TException {
    return mMasterInfo.getFileLocations(fileId);
  }

  @Override
  public List<NetAddress> user_getFileLocationsByPath(String filePath)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.getFileLocations(filePath);
  }

  @Override
  public int user_getFileId(String filePath) throws InvalidPathException, TException {
    return mMasterInfo.getFileId(filePath);
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
    return mMasterInfo.getNewUserId();
  }

  @Override
  public void user_outOfMemoryForPinFile(int fileId) throws TException {
    LOG.error("The user can not allocate enough space for PIN list File " + fileId);
  }

  @Override
  public void user_renameFile(String srcFilePath, String dstFilePath)
      throws FileDoesNotExistException, InvalidPathException, TException {
    mMasterInfo.renameFile(srcFilePath, dstFilePath);
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
    mMasterInfo.unpinFile(fileId);
  }

  @Override
  public void worker_addFile(long workerId, long workerUsedBytes, int fileId,
      int partitionSizeBytes, boolean hasCheckpointed, String checkpointPath)
          throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    mMasterInfo.cachedFile(workerId, workerUsedBytes, fileId, partitionSizeBytes, 
        hasCheckpointed, checkpointPath);
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
    List<Integer> ret = mMasterInfo.getPinList();
    return new HashSet<Integer>(ret);
  }

  @Override
  public Command worker_heartbeat(long workerId, long usedBytes, List<Integer> removedFileIds)
      throws TException {
    return mMasterInfo.workerHeartbeat(workerId, usedBytes, removedFileIds);
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, long totalBytes, long usedBytes,
      List<Integer> currentFileIds) throws TException {
    return mMasterInfo.registerWorker(workerNetAddress, totalBytes, usedBytes, currentFileIds);
  }
}
