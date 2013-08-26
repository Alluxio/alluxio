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

import tachyon.conf.CommonConf;
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
import tachyon.thrift.NoWorkerException;
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
  private final static int MAX_CONNECT_TRY = 5;
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private boolean mUseZookeeper;
  private MasterService.Client mClient = null;
  private InetSocketAddress mZookeeperAddress = null;
  private InetSocketAddress mMasterAddress = null;
  private TProtocol mProtocol = null;
  private boolean mIsConnected;
  private boolean mIsShutdown;
  private long mLastAccessedMs;

  private HeartbeatThread mHeartbeatThread = null;

  public MasterClient(InetSocketAddress masterAddress) {
    this(masterAddress, CommonConf.get().USE_ZOOKEEPER);
  }

  public MasterClient(InetSocketAddress masterAddress, boolean useZookeeper) {
    mUseZookeeper = useZookeeper;
    if (mUseZookeeper) {
      mZookeeperAddress = masterAddress;
    } else {
      mMasterAddress = masterAddress;
    }
    mIsConnected = false;
    mIsShutdown = false;
  }

  private InetSocketAddress getMasterAddress() {
    if (!mUseZookeeper) {
      return mMasterAddress;
    }

    LeaderInquireClient leaderInquireClient = new LeaderInquireClient(
        mZookeeperAddress.toString().substring(1), CommonConf.get().ZOOKEEPER_LEADER_PATH);
    try {
      String temp = leaderInquireClient.getMasterAddress();
      return CommonUtils.parseInetSocketAddress(temp);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      CommonUtils.runtimeException(e);
    }
    return null;
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
    return mClient.addCheckpoint(workerId, fileId, length, checkpointPath);
  }

  /**
   * Try to connect to the master
   * @return true if connection succeed, false otherwise.
   */
  public synchronized boolean connect() {
    if (mIsConnected) {
      return true;
    }
    cleanConnect();
    if (mIsShutdown) {
      return false;
    }

    int tries = 0;
    while (tries ++ < MAX_CONNECT_TRY) {
      mMasterAddress = getMasterAddress();
      mProtocol = new TBinaryProtocol(new TFramedTransport(
          new TSocket(mMasterAddress.getHostName(), mMasterAddress.getPort())));
      mClient = new MasterService.Client(mProtocol);
      mLastAccessedMs = System.currentTimeMillis();
      if (!mIsConnected && !mIsShutdown) {
        try {
          mProtocol.getTransport().open();

          mHeartbeatThread = new HeartbeatThread("Master_Client Heartbeat",
              new MasterClientHeartbeatExecutor(this, UserConf.get().MASTER_CLIENT_TIMEOUT_MS),
              UserConf.get().MASTER_CLIENT_TIMEOUT_MS / 2);
          mHeartbeatThread.start();
        } catch (TTransportException e) {
          LOG.error("Failed to connect (" +tries + ") to master " + mMasterAddress + 
              " : " + e.getMessage(), e);
          CommonUtils.sleepMs(LOG, 1000);
          continue;
        }
        mIsConnected = true;
        break;
      }
    }

    return mIsConnected;
  }

  public synchronized void cleanConnect() {
    if (mIsConnected) {
      LOG.info("Disconnecting from the master " + mMasterAddress);
      mIsConnected = false;
    }
    if (mProtocol != null) {
      mProtocol.getTransport().close();
    }
    if (mHeartbeatThread != null) {
      mHeartbeatThread.shutdown();
    }
  }

  synchronized long getLastAccessedMs() {
    return mLastAccessedMs;
  }

  public synchronized long getUserId() throws TException {
    connect();
    long ret = mClient.user_getUserId();
    LOG.info("User registered at the master " + mMasterAddress + " got UserId " + ret);
    return ret;
  }

  public synchronized List<ClientWorkerInfo> getWorkersInfo() throws TException {
    connect();
    return mClient.getWorkersInfo();
  }

  public synchronized boolean isConnected() {
    return mIsConnected;
  }

  public synchronized void shutdown() {
    mIsShutdown = true;
    cleanConnect();
    mHeartbeatThread.shutdown();
  }

  public synchronized List<ClientFileInfo> listStatus(String path)
      throws IOException, TException {
    connect();
    try {
      return mClient.liststatus(path);
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
      mClient.user_completeFile(fId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_createFile(String path, long blockSizeByte) 
      throws IOException, TException {
    connect();
    try {
      return mClient.user_createFile(path, blockSizeByte);
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
      return mClient.user_createFileOnCheckpoint(path, checkpointPath);
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
      return mClient.user_createNewBlock(fId);
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
      return mClient.user_createRawTable(path, columns, metadata);
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
      return mClient.user_deleteByPath(path, recursive);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized boolean user_delete(int fileId, boolean recursive)
      throws IOException, TException {
    connect();
    try {
      return mClient.user_deleteById(fileId, recursive);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public synchronized long user_getBlockId(int fId, int index)
      throws IOException, TException {
    connect();
    try {
      return mClient.user_getBlockId(fId, index);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public ClientBlockInfo user_getClientBlockInfo(long blockId) 
      throws FileDoesNotExistException, BlockInfoException, TException {
    connect();
    return mClient.user_getClientBlockInfo(blockId);
  }

  public synchronized ClientFileInfo user_getClientFileInfoByPath(String path)
      throws IOException, TException {
    connect();
    try {
      return mClient.user_getClientFileInfoByPath(path);
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
      return mClient.user_getClientFileInfoById(id);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_getFileId(String path) throws IOException, TException {
    connect();
    try {
      return mClient.user_getFileId(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized int user_getRawTableId(String path) throws IOException, TException {
    connect();
    try {
      return mClient.user_getRawTableId(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized List<ClientBlockInfo> user_getFileBlocks(int id) 
      throws IOException, TException {
    connect();
    try {
      return mClient.user_getFileBlocksById(id);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized NetAddress user_getWorker(boolean random, String hostname)
      throws NoWorkerException, TException {
    connect();
    return mClient.user_getWorker(random, hostname);
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws IOException, TException {
    connect();
    ClientRawTableInfo ret;
    try {
      ret = mClient.user_getClientRawTableInfoByPath(path);
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
      ret = mClient.user_getClientRawTableInfoById(id);
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
      return mClient.user_getNumberOfFiles(folderPath);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized String user_getUnderfsAddress() throws TException {
    connect();
    return mClient.user_getUnderfsAddress();
  }

  public synchronized List<Integer> user_listFiles(String path, boolean recursive)
      throws IOException, TException {
    connect();
    try {
      return mClient.user_listFiles(path, recursive);
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
      return mClient.user_ls(path, recursive);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  public synchronized boolean user_mkdir(String path) 
      throws IOException, TException {
    connect();
    try {
      return mClient.user_mkdir(path);
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
    mClient.user_outOfMemoryForPinFile(fileId);
  }

  public synchronized void user_rename(String srcPath, String dstPath)
      throws IOException, TException{
    connect();
    try {
      mClient.user_rename(srcPath, dstPath);
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
      mClient.user_renameTo(fId, path);
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
      mClient.user_unpinFile(id);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    }
  }

  public synchronized void user_updateRawTableMetadata(int id, ByteBuffer metadata)
      throws IOException, TException {
    connect();
    try {
      mClient.user_updateRawTableMetadata(id, metadata);
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
    mClient.worker_cacheBlock(workerId, workerUsedBytes, blockId, length);
  }

  public synchronized Command worker_heartbeat(long workerId, long usedBytes,
      List<Long> removedPartitionList) throws BlockInfoException, TException {
    connect();
    return mClient.worker_heartbeat(workerId, usedBytes, removedPartitionList);
  }

  public synchronized Set<Integer> worker_getPinIdList() throws TException {
    connect();
    return mClient.worker_getPinIdList();
  }

  public synchronized long worker_register(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Long> currentBlockList) throws BlockInfoException, TException {
    connect();
    long ret = mClient.worker_register(workerNetAddress, totalBytes, usedBytes, currentBlockList); 
    LOG.info("Registered at the master " + mMasterAddress + " from worker " + workerNetAddress +
        " , got WorkerId " + ret);
    return ret;
  }
}