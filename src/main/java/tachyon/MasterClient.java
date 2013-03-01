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

  public synchronized int user_createFile(String path)
      throws FileAlreadyExistException, InvalidPathException, TException {
    return CLIENT.user_createFile(path);
  }

  public synchronized int user_createRawTable(String path, int columns, List<Byte> metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException, TException {
    if (metadata == null) {
      metadata = new ArrayList<Byte>(0);
    }
    return CLIENT.user_createRawTable(path, columns, metadata);
  }

  public synchronized void user_delete(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    CLIENT.user_deleteByPath(path);
  }

  public synchronized void user_delete(int fileId) throws FileDoesNotExistException, TException {
    CLIENT.user_deleteById(fileId);
  }

  public synchronized ClientFileInfo user_getClientFileInfoByPath(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return CLIENT.user_getClientFileInfoByPath(path);
  }

  public synchronized ClientFileInfo user_getClientFileInfoById(int id)
      throws FileDoesNotExistException, TException {
    return CLIENT.user_getClientFileInfoById(id);
  }

  public synchronized int user_getFileId(String path) throws InvalidPathException, TException {
    return CLIENT.user_getFileId(path);
  }

  public synchronized int user_getRawTableId(String path) throws InvalidPathException, TException {
    return CLIENT.user_getRawTableId(path);
  }

  public synchronized List<NetAddress> user_getFileLocations(int id)
      throws FileDoesNotExistException, TException {
    return CLIENT.user_getFileLocationsById(id);
  }

  public synchronized NetAddress user_getLocalWorker(String localHostName)
      throws NoLocalWorkerException, TException {
    return CLIENT.user_getLocalWorker(localHostName);
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
    return CLIENT.user_getClientRawTableInfoByPath(path);
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfoById(int id)
      throws TableDoesNotExistException, TException {
    return CLIENT.user_getClientRawTableInfoById(id);
  }

  public synchronized int getNumberOfFiles(String folderPath)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return CLIENT.user_getNumberOfFiles(folderPath);
  }

  public synchronized int user_mkdir(String path) 
      throws FileAlreadyExistException, InvalidPathException, TException {
    return CLIENT.user_mkdir(path);
  }

  public synchronized void user_outOfMemoryForPinFile(int fileId) throws TException {
    CLIENT.user_outOfMemoryForPinFile(fileId);
  }

  public synchronized void user_renameFile(String srcPath, String dstPath)
      throws FileDoesNotExistException, InvalidPathException, TException {
    CLIENT.user_renameFile(srcPath, dstPath);
  }

  public synchronized void user_unpinFile(int id) throws FileDoesNotExistException, TException {
    CLIENT.user_unpinFile(id);
  }

  public synchronized void worker_addCheckpoint(long workerId, int fileId, long fileSizeBytes, 
      String checkpointPath) 
          throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    CLIENT.worker_addCheckpoint(workerId, fileId, fileSizeBytes, checkpointPath);
  }

  public synchronized void worker_cachedFile(long workerId, long workerUsedBytes, int fileId, 
      long fileSizeBytes) throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    CLIENT.worker_cacheFile(workerId, workerUsedBytes, fileId, fileSizeBytes);
  }

  public synchronized Command worker_heartbeat(long workerId, long usedBytes,
      List<Integer> removedPartitionList) throws TException {
    return CLIENT.worker_heartbeat(workerId, usedBytes, removedPartitionList);
  }

  public synchronized Set<Integer> worker_getPinIdList() throws TException {
    return CLIENT.worker_getPinIdList();
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