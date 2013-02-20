package tachyon;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.Command;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.MasterService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

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
    return mMasterInfo.createFile(filePath, false);
  }

  @Override
  public int user_createRawTable(String path, int columns)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException, TException {
    return mMasterInfo.createRawTable(path, columns);
  }

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

  @Override
  public int user_getRawTableId(String path) throws InvalidPathException, TException {
    return mMasterInfo.getRawTableId(path);
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoById(int id)
      throws TableDoesNotExistException, TException {
    return mMasterInfo.getClientRawTableInfo(id);
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.getClientRawTableInfo(path);
  }

  @Override
  public long user_getUserId() throws TException {
    return mMasterInfo.getNewUserId();
  }

  @Override
  public int user_mkdir(String path) 
      throws FileAlreadyExistException, InvalidPathException, TException {
    return mMasterInfo.createFile(path, true);
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
  public Set<Integer> worker_getPinIdList() throws TException {
    List<Integer> ret = mMasterInfo.getPinIdList();
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
