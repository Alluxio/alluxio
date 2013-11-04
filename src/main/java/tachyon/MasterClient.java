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
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.Command;
import tachyon.thrift.DependencyDoesNotExistException;
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
  private InetSocketAddress mMasterAddress = null;
  private TProtocol mProtocol = null;
  private volatile boolean mIsConnected;
  private volatile boolean mIsShutdown;
  private long mLastAccessedMs;

  private HeartbeatThread mHeartbeatThread = null;

  public MasterClient(InetSocketAddress masterAddress) {
    this(masterAddress, CommonConf.get().USE_ZOOKEEPER);
  }

  public MasterClient(InetSocketAddress masterAddress, boolean useZookeeper) {
    mUseZookeeper = useZookeeper;
    if (!mUseZookeeper) {
      mMasterAddress = masterAddress;
    }
    mIsConnected = false;
    mIsShutdown = false;
  }

  private InetSocketAddress getMasterAddress() {
    if (!mUseZookeeper) {
      return mMasterAddress;
    }

    LeaderInquireClient leaderInquireClient = LeaderInquireClient.getClient(
        CommonConf.get().ZOOKEEPER_ADDRESS, CommonConf.get().ZOOKEEPER_LEADER_PATH);
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
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.addCheckpoint(workerId, fileId, length, checkpointPath);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return false;
  }

  /**
   * Try to connect to the master
   * @return true if connection succeed, false otherwise.
   */
  public synchronized boolean connect() {
    mLastAccessedMs = System.currentTimeMillis();
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
              " : " + e.getMessage());
          CommonUtils.sleepMs(LOG, 1000);
          continue;
        }
        mIsConnected = true;
        break;
      }
    }

    return mIsConnected;
  }

  /**
   * Clean the connect. E.g. if the client has not connect the master for a while, the connection
   * should be shut down.
   */
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
    while (!mIsShutdown) {
      connect();
      try {
        long ret = mClient.user_getUserId();
        LOG.info("User registered at the master " + mMasterAddress + " got UserId " + ret);
        return ret;
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized List<ClientWorkerInfo> getWorkersInfo() throws TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.getWorkersInfo();
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized boolean isConnected() {
    return mIsConnected;
  }

  public void shutdown() {
    mIsShutdown = true;
    if (mProtocol != null) {
      mProtocol.getTransport().close();
    }
    cleanConnect();
  }

  public synchronized List<ClientFileInfo> listStatus(String path)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.liststatus(path);
      } catch (InvalidPathException | FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized void user_completeFile(int fId)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_completeFile(fId);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized int user_createFile(String path, long blockSizeByte)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_createFile(path, blockSizeByte);
      } catch (FileAlreadyExistException | InvalidPathException | BlockInfoException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public int user_createFileOnCheckpoint(String path, String checkpointPath)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_createFileOnCheckpoint(path, checkpointPath);
      } catch (FileAlreadyExistException | InvalidPathException | SuspectedFileSizeException |
          BlockInfoException | TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized long user_createNewBlock(int fId) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_createNewBlock(fId);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws IOException, TException {
    if (metadata == null) {
      metadata = ByteBuffer.allocate(0);
    }
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_createRawTable(path, columns, metadata);
      } catch (FileAlreadyExistException | InvalidPathException | TableColumnException |
          TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized boolean user_delete(String path, boolean recursive)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_deleteByPath(path, recursive);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return false;
  }

  public synchronized boolean user_delete(int fileId, boolean recursive)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_deleteById(fileId, recursive);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return false;
  }

  public synchronized long user_getBlockId(int fId, int index)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getBlockId(fId, index);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public ClientBlockInfo user_getClientBlockInfo(long blockId)
      throws FileDoesNotExistException, BlockInfoException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getClientBlockInfo(blockId);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized ClientFileInfo user_getClientFileInfoByPath(String path)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getClientFileInfoByPath(path);
      } catch (FileDoesNotExistException | InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized ClientFileInfo user_getClientFileInfoById(int id)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getClientFileInfoById(id);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized int user_getFileId(String path) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getFileId(path);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized int user_getRawTableId(String path) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getRawTableId(path);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized List<ClientBlockInfo> user_getFileBlocks(int id)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getFileBlocksById(id);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized NetAddress user_getWorker(boolean random, String hostname)
      throws NoWorkerException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getWorker(random, hostname);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        ClientRawTableInfo ret = mClient.user_getClientRawTableInfoByPath(path);
        ret.setMetadata(CommonUtils.generateNewByteBufferFromThriftRPCResults(ret.metadata));
        return ret;
      } catch (TableDoesNotExistException | InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfoById(int id)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        ClientRawTableInfo ret = mClient.user_getClientRawTableInfoById(id);
        ret.setMetadata(CommonUtils.generateNewByteBufferFromThriftRPCResults(ret.metadata));
        return ret;
      } catch (TableDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized int user_getNumberOfFiles(String folderPath)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getNumberOfFiles(folderPath);
      } catch (FileDoesNotExistException | InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized String user_getUnderfsAddress() throws TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getUnderfsAddress();
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized List<Integer> user_listFiles(String path, boolean recursive)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_listFiles(path, recursive);
      } catch (FileDoesNotExistException | InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized List<String> user_ls(String path, boolean recursive)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_ls(path, recursive);
      } catch (FileDoesNotExistException | InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized boolean user_mkdir(String path)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_mkdir(path);
      } catch (FileAlreadyExistException | InvalidPathException | TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return false;
  }

  public synchronized void user_outOfMemoryForPinFile(int fileId) throws TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_outOfMemoryForPinFile(fileId);
        return;
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void user_rename(String srcPath, String dstPath)
      throws IOException, TException{
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_rename(srcPath, dstPath);
        return;
      } catch (FileAlreadyExistException | FileDoesNotExistException | InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public void user_renameTo(int fId, String path) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_renameTo(fId, path);
        return;
      } catch (FileAlreadyExistException | FileDoesNotExistException | InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void user_unpinFile(int id) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_unpinFile(id);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void user_updateRawTableMetadata(int id, ByteBuffer metadata)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_updateRawTableMetadata(id, metadata);
        return;
      } catch (TableDoesNotExistException | TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void worker_cacheBlock(long workerId, long workerUsedBytes, long blockId,
      long length) throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException,
      TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.worker_cacheBlock(workerId, workerUsedBytes, blockId, length);
        return;
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized Command worker_heartbeat(long workerId, long usedBytes,
      List<Long> removedPartitionList) throws BlockInfoException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.worker_heartbeat(workerId, usedBytes, removedPartitionList);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized Set<Integer> worker_getPinIdList() throws TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.worker_getPinIdList();
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  /**
   * Register the worker to the master.
   * @param workerNetAddress Worker's NetAddress
   * @param totalBytes Worker's capacity
   * @param usedBytes Worker's used storage
   * @param currentBlockList Blocks in worker's space.
   * @return the worker id assigned by the master.
   * @throws BlockInfoException
   * @throws TException
   */
  public synchronized long worker_register(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Long> currentBlockList) throws BlockInfoException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        long ret =
            mClient.worker_register(workerNetAddress, totalBytes, usedBytes, currentBlockList);
        LOG.info("Registered at the master " + mMasterAddress + " from worker " + workerNetAddress +
            " , got WorkerId " + ret);
        return ret;
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public synchronized List<Integer> worker_getPriorityDependencyList() throws TException {
    while (true) {
      connect();
      try {
        return mClient.worker_getPriorityDependencyList();
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte)
          throws IOException, TException {
    while (true) {
      connect();
      try {
        return mClient.user_createDependency(parents, children, commandPrefix, data, comment,
            framework, frameworkVersion, dependencyType, childrenBlockSizeByte);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      } catch (InvalidPathException | FileDoesNotExistException | FileAlreadyExistException |
          BlockInfoException | TachyonException e) {
        throw new IOException(e);
      }
    }
  }

  public synchronized void user_reportLostFile(int fileId) throws IOException, TException {
    while (true) {
      connect();
      try {
        mClient.user_reportLostFile(fileId);
        return;
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      }
    }
  }

  public synchronized void user_requestFilesInDependency(int depId)
      throws IOException, TException {
    while (true) {
      connect();
      try {
        mClient.user_requestFilesInDependency(depId);
        return;
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      } catch (DependencyDoesNotExistException e) {
        throw new IOException(e);
      }
    }
  }

  public ClientDependencyInfo getClientDependencyInfo(int did) throws IOException, TException {
    while (true) {
      connect();
      try {
        return mClient.user_getClientDependencyInfo(did);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      } catch (DependencyDoesNotExistException e) {
        throw new IOException(e);
      }
    }
  }
}