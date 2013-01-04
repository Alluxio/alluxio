package tachyon;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.Command;
import tachyon.thrift.InvalidPathException;
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
 * The master server client side.
 * 
 * Since MasterService.Client is not thread safe, this class has to guarantee thread safe.
 * 
 * @author haoyuan
 */
public class MasterClient {
  private final Logger LOG = LoggerFactory.getLogger(MasterClient.class);
  private final MasterService.Client CLIENT;

  private InetSocketAddress mMasterAddress;
  private TProtocol mProtocol;
  private boolean mIsConnected;

  public MasterClient(InetSocketAddress masterAddress) {
    mMasterAddress = masterAddress;
    mProtocol = new TBinaryProtocol(new TFramedTransport(
        new TSocket(mMasterAddress.getHostName(), mMasterAddress.getPort())));
    CLIENT = new MasterService.Client(mProtocol);
    mIsConnected = false;
  }

  public synchronized void close() {
    mProtocol.getTransport().close();
    mIsConnected = false;
  }

  public synchronized List<DatasetInfo> cmd_ls(String folder) throws TException {
    return CLIENT.cmd_ls(folder);
  }

  public synchronized List<DatasetInfo> cmd_rm(String folder)
      throws DatasetDoesNotExistException, TException {
    List<DatasetInfo> ret = CLIENT.cmd_ls(folder);
    for (DatasetInfo dataset : ret) {
      CLIENT.user_deleteDataset(dataset.mId);
    }
    return ret;
  }

  public synchronized long getUserId() throws TException {
    long ret = CLIENT.user_getUserId();
    LOG.info("User registered at the master " + mMasterAddress + " got UserId " + ret);
    return ret;
  }

  public synchronized boolean isConnected() {
    return mIsConnected;
  }

  public synchronized boolean open() {
    if (!mIsConnected) {
      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return false;
      }
      mIsConnected = true;
    }

    return mIsConnected;
  }

  public synchronized int user_createDataset(String datasetPath, int partitions)
      throws DatasetAlreadyExistException, InvalidPathException, TException {
    return CLIENT.user_createDataset(datasetPath, partitions);
  }

  public synchronized int user_createRawColumnDataset(String datasetPath, int columns,
      int partitions) throws DatasetAlreadyExistException, InvalidPathException, TException {
    return CLIENT.user_createRawColumnDataset(datasetPath, columns, partitions);
  }

  public synchronized void user_deleteDataset(int datasetId)
      throws DatasetDoesNotExistException, TException {
    CLIENT.user_deleteDataset(datasetId);
  }

  public synchronized int user_getDatasetId(String datasetPath) throws TException {
    return CLIENT.user_getDatasetId(datasetPath);
  }

  public synchronized NetAddress user_getLocalWorker(String localHostName)
      throws NoLocalWorkerException, TException {
    return CLIENT.user_getLocalWorker(localHostName);
  }

  public synchronized PartitionInfo user_getPartitionInfo(int datasetId, int partitionId
      ) throws PartitionDoesNotExistException, TException {
    return CLIENT.user_getPartitionInfo(datasetId, partitionId);
  }

  public synchronized DatasetInfo user_getDataset(String datasetPath)
      throws DatasetDoesNotExistException, TException {
    return CLIENT.user_getDatasetByPath(datasetPath);
  }

  public synchronized DatasetInfo user_getDataset(int datasetId)
      throws DatasetDoesNotExistException, TException {
    return CLIENT.user_getDatasetById(datasetId);
  }

  public synchronized RawColumnDatasetInfo user_getRawColumnDataset(String datasetPath)
      throws DatasetDoesNotExistException, TException {
    return CLIENT.user_getRawColumnDatasetByPath(datasetPath);
  }

  public synchronized RawColumnDatasetInfo user_getRawColumnDatasetInfo(int datasetId)
      throws DatasetDoesNotExistException, TException {
    return CLIENT.user_getRawColumnDatasetById(datasetId);
  }

  public synchronized void user_outOfMemoryForPinDataset(int datasetId) throws TException {
    CLIENT.user_outOfMemoryForPinDataset(datasetId);
  }

  public synchronized void user_renameDataset(String srcDataset, String dstDataset)
      throws DatasetDoesNotExistException, TException {
    CLIENT.user_renameDataset(srcDataset, dstDataset);
  }

  public synchronized void user_unpinDataset(int datasetId) 
      throws DatasetDoesNotExistException, TException {
    CLIENT.user_unpinDataset(datasetId);
  }
  
  public synchronized void user_setPartitionCheckpointPath(int datasetId, int partitionId,
      String checkpointPath) throws DatasetDoesNotExistException,
      PartitionDoesNotExistException, TException {
    CLIENT.user_setPartitionCheckpointPath(datasetId, partitionId, checkpointPath);
  }

  public synchronized void worker_addPartition(long workerId, long workerUsedBytes,
      int datasetId, int partitionId, int partitionSizeBytes, boolean hasCheckpointed, 
      String checkpointPath)
          throws PartitionDoesNotExistException, SuspectedPartitionSizeException, TException {
    CLIENT.worker_addPartition(workerId, workerUsedBytes, datasetId, partitionId,
        partitionSizeBytes, hasCheckpointed, checkpointPath);
  }

  public synchronized void worker_addRCDPartition(long workerId, int datasetId, int partitionId,
      int partitionSizeBytes) throws PartitionDoesNotExistException,
      SuspectedPartitionSizeException, TException {
    CLIENT.worker_addRCDPartition(workerId, datasetId, partitionId, partitionSizeBytes);
  }

  public synchronized Command worker_heartbeat(long workerId, long usedBytes,
      List<Long> removedPartitionList) throws TException {
    return CLIENT.worker_heartbeat(workerId, usedBytes, removedPartitionList);
  }

  public synchronized Set<Integer> worker_getPinList() throws TException {
    return CLIENT.worker_getPinList();
  }

  public synchronized long worker_register(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Long> currentPartitionList) throws TException {
    long ret = CLIENT.worker_register(
        workerNetAddress, totalBytes, usedBytes, currentPartitionList); 
    LOG.info("Registered at the master " + mMasterAddress + " from worker " + workerNetAddress +
        " , got WorkerId " + ret);
    return ret;
  }

  public static void main(String[] args) {
    int num = Integer.parseInt(args[0]);
    ArrayList<MasterClient> mcs = new ArrayList<MasterClient>(num);
    for (int k = 0; k < num; k ++) {
      mcs.add(new MasterClient(new InetSocketAddress("localhost", Config.MASTER_PORT)));
    }
  }
}