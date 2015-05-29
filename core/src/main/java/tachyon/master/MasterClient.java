/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.security.sasl.SaslException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.HeartbeatThread;
import tachyon.LeaderInquireClient;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.retry.ExponentialBackoffRetry;
import tachyon.retry.RetryPolicy;
import tachyon.security.UserGroup;
import tachyon.security.authentication.AuthenticationFactory;
import tachyon.security.authentication.PlainSaslHelper;
import tachyon.thrift.AccessControlException;
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
// TODO When TException happens, the caller can't really do anything about it.
// when the other exceptions are thrown as a IOException, the caller can't do anything about it
// so all exceptions are handled poorly. This logic needs to be redone and be consistent.
public final class MasterClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final boolean mUseZookeeper;
  private MasterService.Client mClient = null;
  private InetSocketAddress mMasterAddress = null;
  private TProtocol mProtocol = null;
  private volatile boolean mConnected;
  private volatile boolean mIsShutdown;
  private volatile long mUserId = -1;
  private final ExecutorService mExecutorService;
  private Future<?> mHeartbeat;
  private final TachyonConf mTachyonConf;

  public MasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mUseZookeeper = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER, false);
    if (!mUseZookeeper) {
      mMasterAddress = masterAddress;
    }
    mConnected = false;
    mIsShutdown = false;
    mExecutorService = executorService;
  }

  /**
   * @param workerId if -1, means the checkpoint is added directly by the client from underlayer fs.
   * @param fileId
   * @param length
   * @param checkpointPath
   * @return true if checkpoint is added for the <code>fileId</code> and false otherwise
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  public synchronized boolean addCheckpoint(long workerId, int fileId, long length,
      String checkpointPath) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.addCheckpoint(workerId, fileId, length, checkpointPath);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (SuspectedFileSizeException e) {
        throw new IOException(e);
      } catch (BlockInfoException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return false;
  }

  /**
   * Clean the connect. E.g. if the client has not connect the master for a while, the connection
   * should be shut down.
   */
  @Override
  public synchronized void close() {
    if (mConnected) {
      LOG.debug("Disconnecting from the master {}", mMasterAddress);
      mConnected = false;
    }
    try {
      if (mProtocol != null) {
        mProtocol.getTransport().close();
      }
    } finally {
      if (mHeartbeat != null) {
        mHeartbeat.cancel(true);
      }
    }
  }

  /**
   * Connects to the Tachyon Master; an exception is thrown if this fails.
   */
  public synchronized void connect() throws IOException {
    if (mConnected) {
      return;
    }

    close();

    if (mIsShutdown) {
      throw new IOException("Client is shutdown, will not try to connect");
    }

    Exception lastException = null;
    int maxConnectsTry = mTachyonConf.getInt(Constants.MASTER_RETRY_COUNT, 29);
    RetryPolicy retry = new ExponentialBackoffRetry(50, Constants.SECOND_MS, maxConnectsTry);
    do {
      mMasterAddress = getMasterAddress();

      LOG.info("Tachyon client (version " + Version.VERSION + ") is trying to connect master @ "
          + mMasterAddress);

      mProtocol = new TBinaryProtocol(createTransport());
      mClient = new MasterService.Client(mProtocol);
      try {
        mProtocol.getTransport().open();

        HeartbeatExecutor heartBeater = new MasterClientHeartbeatExecutor(this);

        String threadName = "master-heartbeat-" + mMasterAddress;
        int interval = mTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS,
            Constants.SECOND_MS);
        mHeartbeat =
            mExecutorService.submit(new HeartbeatThread(threadName, heartBeater,
                interval / 2));
      } catch (TTransportException e) {
        lastException = e;
        LOG.error("Failed to connect (" + retry.getRetryCount() + ") to master " + mMasterAddress 
            + " : " + e.getMessage());
        if (mHeartbeat != null) {
          mHeartbeat.cancel(true);
        }
        continue;
      }

      try {
        mUserId = mClient.user_getUserId();
      } catch (TException e) {
        lastException = e;
        LOG.error(e.getMessage(), e);
        continue;
      }
      LOG.info("User registered at the master " + mMasterAddress + " got UserId " + mUserId);

      mConnected = true;
      return;
    } while (retry.attemptRetry() && !mIsShutdown);

    // Reaching here indicates that we did not successfully connect.
    throw new IOException("Failed to connect to master " + mMasterAddress + " after "
        + (retry.getRetryCount()) + " attempts", lastException);
  }

  /**
   * Create transport per the connection options Supported transport options are: - SASL based
   * transports over + Kerberos + Delegation token + SSL + non-SSL - Raw (non-SASL) socket
   * 
   * Kerberos and Delegation token supports SASL QOP configurations
   * 
   * @throws TTransportException
   */
  private TTransport createTransport() throws IOException {
    TTransport tTransport = null;
    String authTypeStr =
        mTachyonConf.get(Constants.TACHYON_SECURITY_AUTHENTICATION,
            AuthenticationFactory.AuthTypes.NOSASL.getAuthName());
    try {
      if (authTypeStr.equalsIgnoreCase(AuthenticationFactory.AuthTypes.KERBEROS.getAuthName())) {
        // TODO: Kerboros
      } else if (authTypeStr.equalsIgnoreCase(
          AuthenticationFactory.AuthTypes.SIMPLE.getAuthName())) {
        String username = getUserName();

        if (mTachyonConf.getBoolean(Constants.TACHYON_SECURITY_USE_SSL, false)) {
          // TODO: ssl
        } else {
          tTransport = AuthenticationFactory.createTSocket(mMasterAddress);
        }
        // Overlay the SASL transport on top of the base socket transport (SSL or non-SSL)
        tTransport = PlainSaslHelper.getPlainTransport(username, "noPassword", tTransport);
      } else if (authTypeStr.equalsIgnoreCase(
          AuthenticationFactory.AuthTypes.NOSASL.getAuthName())) {
        tTransport = new TFramedTransport(AuthenticationFactory.createTSocket(mMasterAddress));
      }
    } catch (SaslException e) {
      throw e;
    }
    return tTransport;
  }

  private String getUserName() throws IOException {
    // TODO: high layer user

    // Login user
    return UserGroup.getTachyonLoginUser().getShortUserName();
  }

  public synchronized ClientDependencyInfo getClientDependencyInfo(int did) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_getClientDependencyInfo(did);
      } catch (DependencyDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized ClientFileInfo getFileStatus(int fileId, String path) throws IOException {
    if (path == null) {
      path = "";
    }
    if (fileId == -1 && !path.startsWith(TachyonURI.SEPARATOR)) {
      throw new IOException("Illegal path parameter: " + path);
    }

    while (!mIsShutdown) {
      connect();

      try {
        return mClient.getFileStatus(fileId, path);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  private synchronized InetSocketAddress getMasterAddress() {
    if (!mUseZookeeper) {
      return mMasterAddress;
    }

    LeaderInquireClient leaderInquireClient =
        LeaderInquireClient.getClient(mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS, null),
            mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH, null));
    try {
      String temp = leaderInquireClient.getMasterAddress();
      return CommonUtils.parseInetSocketAddress(temp);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  public synchronized long getUserId() throws IOException {
    while (!mIsShutdown) {
      connect();

      return mUserId;
    }

    return -1;
  }

  public synchronized List<ClientWorkerInfo> getWorkersInfo() throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.getWorkersInfo();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized boolean isConnected() {
    return mConnected;
  }

  public synchronized List<ClientFileInfo> listStatus(String path) throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.liststatus(path);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  private synchronized void parameterCheck(int id, String path) throws IOException {
    if (path == null) {
      throw new NullPointerException("Paths may not be null; empty is the null state");
    }
    if (id == -1 && !path.startsWith(TachyonURI.SEPARATOR)) {
      throw new IOException("Illegal path parameter: " + path);
    }
  }

  public synchronized void shutdown() {
    close();
    mIsShutdown = true;
  }

  public synchronized void user_completeFile(int fId) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        mClient.user_completeFile(fId);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
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
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized int user_createFile(String path, String ufsPath, long blockSizeByte,
      boolean recursive) throws IOException {
    if (path == null || !path.startsWith(TachyonURI.SEPARATOR)) {
      throw new IOException("Illegal path parameter: " + path);
    }
    if (ufsPath == null) {
      ufsPath = "";
    }

    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_createFile(path, ufsPath, blockSizeByte, recursive);
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (BlockInfoException e) {
        throw new IOException(e);
      } catch (SuspectedFileSizeException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized long user_createNewBlock(int fId) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_createNewBlock(fId);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws IOException {
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
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized boolean user_delete(int fileId, String path, boolean recursive)
      throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_delete(fileId, path, recursive);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return false;
  }

  public synchronized long user_getBlockId(int fId, int index) throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getBlockId(fId, index);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized ClientBlockInfo user_getClientBlockInfo(long blockId) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_getClientBlockInfo(blockId);
      } catch (FileDoesNotExistException e) {
        throw new FileNotFoundException(e.getMessage());
      } catch (BlockInfoException e) {
        throw new IOException(e.getMessage(), e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized ClientRawTableInfo user_getClientRawTableInfo(int id, String path)
      throws IOException {
    parameterCheck(id, path);

    while (!mIsShutdown) {
      connect();

      try {
        ClientRawTableInfo ret = mClient.user_getClientRawTableInfo(id, path);
        ret.setMetadata(CommonUtils.generateNewByteBufferFromThriftRPCResults(ret.metadata));
        return ret;
      } catch (TableDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized List<ClientBlockInfo> user_getFileBlocks(int fileId, String path)
      throws IOException {
    parameterCheck(fileId, path);

    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_getFileBlocks(fileId, path);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized int user_getRawTableId(String path) throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_getRawTableId(path);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized String user_getUfsAddress() throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_getUfsAddress();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized NetAddress user_getWorker(boolean random, String hostname)
      throws NoWorkerException, IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_getWorker(random, hostname);
      } catch (NoWorkerException e) {
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized void user_heartbeat() throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        mClient.user_heartbeat();
        return;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized boolean user_mkdirs(String path, boolean recursive) throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_mkdirs(path, recursive);
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return false;
  }

  public synchronized boolean user_rename(int fileId, String srcPath, String dstPath)
      throws IOException {
    parameterCheck(fileId, srcPath);

    while (!mIsShutdown) {
      connect();

      try {
        return mClient.user_rename(fileId, srcPath, dstPath);
      } catch (FileAlreadyExistException e) {
        throw new IOException(e);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return false;
  }

  public synchronized void user_reportLostFile(int fileId) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        mClient.user_reportLostFile(fileId);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized void user_requestFilesInDependency(int depId) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        mClient.user_requestFilesInDependency(depId);
        return;
      } catch (DependencyDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized void user_setPinned(int id, boolean pinned) throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        mClient.user_setPinned(id, pinned);
        return;
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized void user_updateRawTableMetadata(int id, ByteBuffer metadata)
      throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        mClient.user_updateRawTableMetadata(id, metadata);
        return;
      } catch (TableDoesNotExistException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized boolean user_freepath(int fileId, String path, boolean recursive)
      throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_freepath(fileId, path, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return false;
  }

  public synchronized void worker_cacheBlock(long workerId, long usedBytesOnTier, long storageDirId,
      long blockId, long length) throws IOException, FileDoesNotExistException,
      BlockInfoException {
    while (!mIsShutdown) {
      connect();

      try {
        mClient.worker_cacheBlock(workerId, usedBytesOnTier, storageDirId, blockId, length);
        return;
      } catch (FileDoesNotExistException e) {
        throw e;
      } catch (BlockInfoException e) {
        throw e;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
  }

  public synchronized Set<Integer> worker_getPinIdList() throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.worker_getPinIdList();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  public synchronized List<Integer> worker_getPriorityDependencyList() throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.worker_getPriorityDependencyList();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return new ArrayList<Integer>();
  }

  public synchronized Command worker_heartbeat(long workerId, List<Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<Long, List<Long>> addedBlockIds)
      throws IOException {
    while (!mIsShutdown) {
      connect();

      try {
        return mClient.worker_heartbeat(workerId, usedBytesOnTiers, removedBlockIds, addedBlockIds);
      } catch (BlockInfoException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  /**
   * Register the worker to the master.
   * 
   * @param workerNetAddress Worker's NetAddress
   * @param totalBytesOnTiers Total bytes on each storage tier
   * @param usedBytesOnTiers Used bytes on each storage tier
   * @param currentBlockList Blocks in worker's space.
   * @return the worker id assigned by the master.
   * @throws BlockInfoException
   * @throws TException
   */
  public synchronized long worker_register(NetAddress workerNetAddress,
      List<Long> totalBytesOnTiers, List<Long> usedBytesOnTiers,
      Map<Long, List<Long>> currentBlockList) throws BlockInfoException, IOException {
    while (!mIsShutdown) {
      connect();

      try {
        long ret = mClient.worker_register(workerNetAddress, totalBytesOnTiers, usedBytesOnTiers,
            currentBlockList);
        LOG.info("Registered at the master " + mMasterAddress + " from worker " + workerNetAddress
            + " , got WorkerId " + ret);
        return ret;
      } catch (BlockInfoException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized boolean user_setOwner(int fileId, String path, String username,
      String groupname, boolean recursive) throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_setOwner(fileId, path, username, groupname, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return false;
  }

  public synchronized boolean user_setPermission(int fileId, String path, short permission,
      boolean recursive) throws IOException {
    while (!mIsShutdown) {
      connect();
      try {
        return mClient.user_setPermission(fileId, path, permission, recursive);
      } catch (FileDoesNotExistException e) {
        throw new IOException(e);
      } catch (InvalidPathException e) {
        throw new IOException(e);
      } catch (AccessControlException e) {
        throw new IOException(e);
      } catch (TachyonException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return false;
  }
}
