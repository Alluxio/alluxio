/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.HeartbeatThread;
import tachyon.LeaderInquireClient;
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
import tachyon.util.CommonUtils;

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
  private volatile long mLastAccessedMs;

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

  /**
   * @param workerId
   *          if -1, means the checkpoint is added directly by the client from underlayer fs.
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
      String checkpointPath) throws FileDoesNotExistException, SuspectedFileSizeException,
      BlockInfoException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.addCheckpoint(workerId, fileId, length, checkpointPath);
      } catch (TException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return false;
  }

  /**
   * Clean the connect. E.g. if the client has not connect the master for a while, the connection
   * should be shut down.
   */
  public synchronized void cleanConnect() {
    if (mIsConnected) {
      LOG.debug("Disconnecting from the master " + mMasterAddress);
      mIsConnected = false;
    }
    if (mProtocol != null) {
      mProtocol.getTransport().close();
    }
    if (mHeartbeatThread != null) {
      mHeartbeatThread.shutdown();
    }
  }

  /**
   * Connects to the Tachyon Master; an exception is thrown if this fails.
   */
  public synchronized void connect() throws TException {
    mLastAccessedMs = System.currentTimeMillis();
    if (mIsConnected) {
      return;
    }
    cleanConnect();
    if (mIsShutdown) {
      throw new TException("Client is shutdown, will not try to connect");
    }

    int tries = 0;
    Exception lastException = null;
    while (tries ++ < MAX_CONNECT_TRY && !mIsShutdown) {
      mMasterAddress = getMasterAddress();
      mProtocol =
          new TBinaryProtocol(new TFramedTransport(new TSocket(mMasterAddress.getHostName(),
              mMasterAddress.getPort())));
      mClient = new MasterService.Client(mProtocol);
      mLastAccessedMs = System.currentTimeMillis();
      try {
        mProtocol.getTransport().open();

        mHeartbeatThread =
            new HeartbeatThread("Master_Client Heartbeat", new MasterClientHeartbeatExecutor(this,
                UserConf.get().MASTER_CLIENT_TIMEOUT_MS),
                UserConf.get().MASTER_CLIENT_TIMEOUT_MS / 2);
        mHeartbeatThread.start();
      } catch (TTransportException e) {
        lastException = e;
        LOG.error("Failed to connect (" + tries + ") to master " + mMasterAddress + " : "
            + e.getMessage());
        if (mHeartbeatThread != null) {
          mHeartbeatThread.shutdown();
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        continue;
      }
      mIsConnected = true;
      return;
    }

    // Reaching here indicates that we did not successfully connect.
    throw new TException("Failed to connect to master " + mMasterAddress + " after " + (tries - 1)
        + " attempts", lastException);
  }

  public ClientDependencyInfo getClientDependencyInfo(int did) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getClientDependencyInfo(did);
      } catch (DependencyDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized ClientFileInfo getClientFileInfoById(int id) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.getClientFileInfoById(id);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  long getLastAccessedMs() {
    return mLastAccessedMs;
  }

  private InetSocketAddress getMasterAddress() {
    if (!mUseZookeeper) {
      return mMasterAddress;
    }

    LeaderInquireClient leaderInquireClient =
        LeaderInquireClient.getClient(CommonConf.get().ZOOKEEPER_ADDRESS,
            CommonConf.get().ZOOKEEPER_LEADER_PATH);
    try {
      String temp = leaderInquireClient.getMasterAddress();
      return CommonUtils.parseInetSocketAddress(temp);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      CommonUtils.runtimeException(e);
    }
    return null;
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

  public synchronized List<ClientFileInfo> listStatus(String path) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.liststatus(path);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public void shutdown() {
    mIsShutdown = true;
    if (mProtocol != null) {
      mProtocol.getTransport().close();
    }
    cleanConnect();
  }

  public synchronized void user_completeFile(int fId) throws IOException, TException {
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

  public synchronized int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException,
      TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_createDependency(parents, children, commandPrefix, data, comment,
            framework, frameworkVersion, dependencyType, childrenBlockSizeByte);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (BlockInfoException e) {
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

  public synchronized int user_createFile(String path, long blockSizeByte) throws IOException,
      TException {
    while (!mIsShutdown) {
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
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }

  public int user_createFileOnCheckpoint(String path, String checkpointPath) throws IOException,
      TException {
    while (!mIsShutdown) {
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
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TableColumnException e) {
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

  public synchronized boolean user_delete(int fileId, boolean recursive) throws IOException,
      TException {
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

  public synchronized boolean user_delete(String path, boolean recursive) throws IOException,
      TException {
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

  public synchronized long user_getBlockId(int fId, int index) throws IOException, TException {
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

  public ClientBlockInfo user_getClientBlockInfo(long blockId) throws FileDoesNotExistException,
      BlockInfoException, TException {
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

  public synchronized ClientFileInfo user_getClientFileInfoByPath(String path) throws IOException,
      TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getClientFileInfoByPath(path);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
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

  public synchronized ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        ClientRawTableInfo ret = mClient.user_getClientRawTableInfoByPath(path);
        ret.setMetadata(CommonUtils.generateNewByteBufferFromThriftRPCResults(ret.metadata));
        return ret;
      } catch (TableDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized List<ClientBlockInfo> user_getFileBlocks(int id) throws IOException,
      TException {
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

  public synchronized int user_getNumberOfFiles(String folderPath) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getNumberOfFiles(folderPath);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
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

  public synchronized List<Integer> user_listFiles(String path, boolean recursive)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_listFiles(path, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized List<String> user_ls(String path, boolean recursive) throws IOException,
      TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_ls(path, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return null;
  }

  public synchronized boolean user_mkdir(String path) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_mkdir(path);
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
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

  public synchronized boolean user_rename(String srcPath, String dstPath) throws IOException,
      TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_rename(srcPath, dstPath);
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return false;
  }

  public void user_renameTo(int fId, String path) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_renameTo(fId, path);
        return;
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void user_reportLostFile(int fileId) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_reportLostFile(fileId);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void user_requestFilesInDependency(int depId) throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_requestFilesInDependency(depId);
        return;
      } catch (DependencyDoesNotExistException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void user_setPinned(int id, boolean pinned)
      throws IOException, TException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_setPinned(id, pinned);
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
      } catch (TableDoesNotExistException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
  }

  public synchronized void worker_cacheBlock(long workerId, long workerUsedBytes, long blockId,
      long length) throws FileDoesNotExistException, SuspectedFileSizeException,
      BlockInfoException, TException {
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

  public synchronized List<Integer> worker_getPriorityDependencyList() throws TException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.worker_getPriorityDependencyList();
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return new ArrayList<Integer>();
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

  /**
   * Register the worker to the master.
   * 
   * @param workerNetAddress
   *          Worker's NetAddress
   * @param totalBytes
   *          Worker's capacity
   * @param usedBytes
   *          Worker's used storage
   * @param currentBlockList
   *          Blocks in worker's space.
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
        LOG.info("Registered at the master " + mMasterAddress + " from worker " + workerNetAddress
            + " , got WorkerId " + ret);
        return ret;
      } catch (TTransportException e) {
        LOG.error(e.getMessage());
        mIsConnected = false;
      }
    }
    return -1;
  }
}