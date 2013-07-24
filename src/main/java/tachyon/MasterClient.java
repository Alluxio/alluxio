package tachyon;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.log4j.Logger;

import tachyon.conf.UserConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
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
import tachyon.thrift.TachyonException;

/**
 * The master server client side.
 * 
 * Since MasterService.Client is not thread safe, this class has to guarantee thread safe.
 */
public class MasterClient {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final MasterService.Client CLIENT;

  private InetSocketAddress mMasterAddress;
  private TProtocol mProtocol;
  private boolean mIsConnected;
  private boolean mIsShutdown;
  private long mLastAccessedMs;

  private HeartbeatThread mHeartbeatThread;

  public MasterClient(InetSocketAddress masterAddress) {
    mMasterAddress = masterAddress;
    mProtocol = new TBinaryProtocol(new TFramedTransport(
        new TSocket(mMasterAddress.getHostName(), mMasterAddress.getPort())));
    CLIENT = new MasterService.Client(mProtocol);
    mIsConnected = false;
    mIsShutdown = false;

    mHeartbeatThread = new HeartbeatThread("Master_Client Heartbeat",
        new MasterClientHeartbeatExecutor(this, UserConf.get().MASTER_CLIENT_TIMEOUT_MS),
        UserConf.get().MASTER_CLIENT_TIMEOUT_MS / 2);
    mHeartbeatThread.start();
  }

  /**
   * @param workerId if -1, means the checkpoint is added directly by the client from underlayer fs.
   * @param fileId
   * @param length
   * @param checkpointPath
   * @return
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException 
   * @throws TException
   */
  public synchronized boolean addCheckpoint(long workerId, int fileId, long length, 
      String checkpointPath)
          throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException,
          TException {
    connect();
    return CLIENT.addCheckpoint(workerId, fileId, length, checkpointPath);
  }

  public synchronized boolean connect() {
    mLastAccessedMs = System.currentTimeMillis();
    if (!mIsConnected && !mIsShutdown) {
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

  public synchronized void disconnect() {
    if (mIsConnected) {
      mProtocol.getTransport().close();
      mIsConnected = false;
    }
  }

  synchronized long getLastAccessedMs() {
    return mLastAccessedMs;
  }

  public synchronized long getUserId() throws TException {
    connect();
    long ret = CLIENT.user_getUserId();
    LOG.info("User registered at the master " + mMasterAddress + " got UserId " + ret);
    return ret;
  }

  public synchronized List<ClientWorkerInfo> getWorkersInfo() throws TException {
    connect();
    return CLIENT.getWorkersInfo();
  }

  public synchronized boolean isConnected() {
    return mIsConnected;
  }

  public synchronized void shutdown() {
    mIsShutdown = true;
    disconnect();
    mHeartbeatThread.shutdown();
  }

  public synchronized List<ClientFileInfo> listStatus(String path)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.liststatus(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized void user_completeFile(int fId)
      throws IOException, TException {
    connect();
    try {
      CLIENT.user_completeFile(fId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_createFile(String path, long blockSizeByte) 
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_createFile(path, blockSizeByte);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public int user_createFileOnCheckpoint(String path, String checkpointPath)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_createFileOnCheckpoint(path, checkpointPath);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized long user_createNewBlock(int fId) throws IOException, TException {
    connect();
    try {
      return CLIENT.user_createNewBlock(fId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws IOException, TException {
    if (metadata == null) {
      metadata = ByteBuffer.allocate(0);
    }
    connect();
    try {
      return CLIENT.user_createRawTable(path, columns, metadata);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (TableColumnException e) {
      throw new IOException(e);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized boolean user_delete(String path, boolean recursive) 
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_deleteByPath(path, recursive);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized boolean user_delete(int fileId, boolean recursive)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_deleteById(fileId, recursive);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized long user_getBlockId(int fId, int index)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_getBlockId(fId, index);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public ClientBlockInfo user_getClientBlockInfo(long blockId) 
      throws FileDoesNotExistException, BlockInfoException, TException {
    connect();
    return CLIENT.user_getClientBlockInfo(blockId);
  }

  public synchronized ClientFileInfo user_getClientFileInfoByPath(String path)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_getClientFileInfoByPath(path);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized ClientFileInfo user_getClientFileInfoById(int id)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_getClientFileInfoById(id);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_getFileId(String path) throws IOException, TException {
    connect();
    try {
      return CLIENT.user_getFileId(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_getRawTableId(String path) throws IOException, TException {
    connect();
    try {
      return CLIENT.user_getRawTableId(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized List<ClientBlockInfo> user_getFileBlocks(int id) 
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_getFileBlocksById(id);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized NetAddress user_getWorker(boolean random, String hostname)
      throws NoLocalWorkerException, TException {
    connect();
    return CLIENT.user_getWorker(random, hostname);
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws IOException, TException {
    connect();
    ClientRawTableInfo ret;
    try {
      ret = CLIENT.user_getClientRawTableInfoByPath(path);
    } catch (TableDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
    ret.setMetadata(CommonUtils.generateNewByteBufferFromThriftRPCResults(ret.metadata));
    return ret;
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfoById(int id)
      throws IOException, TException {
    connect();
    ClientRawTableInfo ret;
    try {
      ret = CLIENT.user_getClientRawTableInfoById(id);
    } catch (TableDoesNotExistException e) {
      throw new IOException(e);
    }
    ret.setMetadata(CommonUtils.generateNewByteBufferFromThriftRPCResults(ret.metadata));
    return ret;
  }

  public synchronized int user_getNumberOfFiles(String folderPath)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_getNumberOfFiles(folderPath);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized String user_getUnderfsAddress() throws TException {
    connect();
    return CLIENT.user_getUnderfsAddress();
  }

  public synchronized List<Integer> user_listFiles(String path, boolean recursive)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_listFiles(path, recursive);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized List<String> user_ls(String path, boolean recursive)
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_ls(path, recursive);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_mkdir(String path) 
      throws IOException, TException {
    connect();
    try {
      return CLIENT.user_mkdir(path);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized void user_outOfMemoryForPinFile(int fileId) throws TException {
    connect();
    CLIENT.user_outOfMemoryForPinFile(fileId);
  }

  public synchronized void user_rename(String srcPath, String dstPath)
      throws IOException, TException{
    connect();
    try {
      CLIENT.user_rename(srcPath, dstPath);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public void user_renameTo(int fId, String path) throws IOException, TException {
    connect();
    try {
      CLIENT.user_renameTo(fId, path);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized void user_unpinFile(int id) throws IOException, TException {
    connect();
    try {
      CLIENT.user_unpinFile(id);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized void user_updateRawTableMetadata(int id, ByteBuffer metadata)
      throws IOException, TException {
    connect();
    try {
      CLIENT.user_updateRawTableMetadata(id, metadata);
    } catch (TableDoesNotExistException e) {
      throw new IOException(e);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized void worker_cacheBlock(long workerId, long workerUsedBytes, long blockId, 
      long length) throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, 
      TException {
    connect();
    CLIENT.worker_cacheBlock(workerId, workerUsedBytes, blockId, length);
  }

  public synchronized Command worker_heartbeat(long workerId, long usedBytes,
      List<Long> removedPartitionList) throws BlockInfoException, TException {
    connect();
    return CLIENT.worker_heartbeat(workerId, usedBytes, removedPartitionList);
  }

  public synchronized Set<Integer> worker_getPinIdList() throws TException {
    connect();
    return CLIENT.worker_getPinIdList();
  }

  public synchronized long worker_register(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Long> currentBlockList) throws BlockInfoException, TException {
    connect();
    long ret = CLIENT.worker_register(workerNetAddress, totalBytes, usedBytes, currentBlockList); 
    LOG.info("Registered at the master " + mMasterAddress + " from worker " + workerNetAddress +
        " , got WorkerId " + ret);
    return ret;
  }
}