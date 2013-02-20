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

  public synchronized List<String> ls(String folder)
      throws InvalidPathException, FileDoesNotExistException, TException {
    return CLIENT.cmd_ls(folder);
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

  public synchronized int user_createFile(String filePath)
      throws FileAlreadyExistException, InvalidPathException, TException {
    return CLIENT.user_createFile(filePath);
  }

  //  public synchronized int user_createRawColumnDataset(String datasetPath, int columns,
  //      int partitions) throws DatasetAlreadyExistException, InvalidPathException, TException {
  //    return CLIENT.user_createRawColumnDataset(datasetPath, columns, partitions);
  //  }

  public synchronized void user_delete(String folder)
      throws FileDoesNotExistException, InvalidPathException, TException {
    CLIENT.user_deleteByPath(folder);
  }

  public synchronized void user_delete(int fileId) throws FileDoesNotExistException, TException {
    CLIENT.user_deleteById(fileId);
  }

  public ClientFileInfo user_getClientFileInfoByPath(String filePath)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return CLIENT.user_getClientFileInfoByPath(filePath);
  }

  public ClientFileInfo user_getClientFileInfoById(int fileId)
      throws FileDoesNotExistException, TException {
    return CLIENT.user_getClientFileInfoById(fileId);
  }

  public synchronized int user_getFileId(String filePath) throws InvalidPathException, TException {
    return CLIENT.user_getFileId(filePath);
  }

  public synchronized List<NetAddress> user_getFileLocations(int fileId)
      throws FileDoesNotExistException, TException {
    return CLIENT.user_getFileLocationsById(fileId);
  }

  public synchronized NetAddress user_getLocalWorker(String localHostName)
      throws NoLocalWorkerException, TException {
    return CLIENT.user_getLocalWorker(localHostName);
  }

  //  public synchronized RawColumnDatasetInfo user_getRawColumnDataset(String datasetPath)
  //      throws FileDoesNotExistException, TException {
  //    return CLIENT.user_getRawColumnDatasetByPath(datasetPath);
  //  }
  //
  //  public synchronized RawColumnDatasetInfo user_getRawColumnDatasetInfo(int fileId)
  //      throws FileDoesNotExistException, TException {
  //    return CLIENT.user_getRawColumnDatasetById(fileId);
  //  }

  public synchronized void user_outOfMemoryForPinDataset(int fileId) throws TException {
    CLIENT.user_outOfMemoryForPinFile(fileId);
  }

  public synchronized void user_renameFile(String srcDataset, String dstDataset)
      throws FileDoesNotExistException, InvalidPathException, TException {
    CLIENT.user_renameFile(srcDataset, dstDataset);
  }

  public synchronized void user_unpinFile(int fileId) 
      throws FileDoesNotExistException, TException {
    CLIENT.user_unpinFile(fileId);
  }

  //  public synchronized void user_setPartitionCheckpointPath(int fileId, int partitionId,
  //      String checkpointPath) throws FileDoesNotExistException,
  //      FileDoesNotExistException, TException {
  //    CLIENT.user_setFileCheckpointPath(fileId, partitionId, checkpointPath);
  //  }

  public synchronized void worker_addFile(long workerId, long workerUsedBytes,
      int fileId, int partitionSizeBytes, boolean hasCheckpointed, 
      String checkpointPath)
          throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    CLIENT.worker_addFile(workerId, workerUsedBytes, fileId, partitionSizeBytes, 
        hasCheckpointed, checkpointPath);
  }

  //  public synchronized void worker_addRCDPartition(long workerId, int fileId, int partitionId,
  //      int partitionSizeBytes) throws FileDoesNotExistException,
  //      SuspectedFileSizeException, TException {
  //    CLIENT.worker_addRCDPartition(workerId, fileId, partitionId, partitionSizeBytes);
  //  }

  public synchronized Command worker_heartbeat(long workerId, long usedBytes,
      List<Integer> removedPartitionList) throws TException {
    return CLIENT.worker_heartbeat(workerId, usedBytes, removedPartitionList);
  }

  public synchronized Set<Integer> worker_getPinList() throws TException {
    return CLIENT.worker_getPinList();
  }

  public synchronized long worker_register(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Integer> currentFileList) throws TException {
    long ret = CLIENT.worker_register(
        workerNetAddress, totalBytes, usedBytes, currentFileList); 
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