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

package tachyon.worker;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.HeartbeatThread;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.OutOfSpaceException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.ClientMetrics;
import tachyon.worker.WorkerClientHeartbeatExecutor;

/**
 * The client talks to a worker server. It keeps sending keep alive message to the worker server.
 *
 * Since WorkerService.Client is not thread safe, this class has to guarantee thread safety.
 */
public class WorkerClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int CONNECTION_RETRY_TIMES = 5;

  private final NetAddress mWorkerNetAddress;
  private final boolean mIsLocal;

  private WorkerService.Client mClient;
  private TProtocol mProtocol;
  private long mUserId;
  private InetSocketAddress mWorkerAddress;
  // This is the address of the data server on the worker.
  private InetSocketAddress mWorkerDataServerAddress;
  // TODO: This boolean indicates whether or not the client is connected to the worker. However,
  // since error exceptions are returned through thrift, all api errors look like fatal errors like
  // network/thrift problems. Maybe error codes/status should be returned for api errors, to be
  // independent from thrift exceptions.
  private boolean mConnected = false;
  private final ExecutorService mExecutorService;
  private Future<?> mHeartbeat;
  private HeartbeatExecutor mHeartbeatExecutor;

  private final TachyonConf mTachyonConf;
  private final ClientMetrics mClientMetrics;

  /**
   * Create a WorkerClient, with a given MasterClient.
   *
   * @param workerNetAddress
   * @param executorService
   * @param conf
   * @param userId
   * @param clientMetrics
   */
  public WorkerClient(NetAddress workerNetAddress, ExecutorService executorService,
      TachyonConf conf, long userId, boolean isLocal, ClientMetrics clientMetrics) {
    mWorkerNetAddress = workerNetAddress;
    mExecutorService = executorService;
    mTachyonConf = conf;
    mUserId = userId;
    mIsLocal = isLocal;
    mClientMetrics = clientMetrics;
  }

  /**
   * Update the latest block access time on the worker.
   *
   * @param blockId The id of the block
   * @throws IOException
   */
  public synchronized void accessBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.accessBlock(blockId);
    } catch (TException e) {
      LOG.error("TachyonClient accessLocalBlock(" + blockId + ") failed");
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker that the checkpoint file of the file has been added.
   *
   * @param fileId The id of the checkpointed file
   * @throws IOException
   */
  public synchronized void addCheckpoint(long fileId) throws IOException {
    mustConnect();

    try {
      mClient.addCheckpoint(mUserId, fileId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    } catch (FailedToCheckpointException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker to checkpoint the file asynchronously.
   *
   * @param fileId The id of the file
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean asyncCheckpoint(long fileId) throws IOException {
    mustConnect();

    try {
      return mClient.asyncCheckpoint(fileId);
    } catch (TachyonException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker the block is cached.
   *
   * @param blockId The id of the block
   * @throws IOException
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.cacheBlock(mUserId, blockId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify worker that the block has been cancelled
   *
   * @param blockId The Id of the block to be cancelled
   * @throws IOException
   */
  public synchronized void cancelBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.cancelBlock(mUserId, blockId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Close the connection to worker. Shutdown the heartbeat thread.
   */
  @Override
  public synchronized void close() {
    if (mConnected) {
      try {
        // Heartbeat to send the client metrics.
        if (mHeartbeatExecutor != null) {
          mHeartbeatExecutor.heartbeat();
        }
        mProtocol.getTransport().close();
      } finally {
        if (mHeartbeat != null) {
          mHeartbeat.cancel(true);
        }
      }
      mConnected = false;
    }
  }

  /**
   * Open the connection to the worker. And start the heartbeat thread.
   *
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  private synchronized boolean connect() throws IOException {
    if (!mConnected) {
      String host = NetworkAddressUtils.getFqdnHost(mWorkerNetAddress);
      int port = mWorkerNetAddress.mPort;
      mWorkerAddress = new InetSocketAddress(host, port);
      mWorkerDataServerAddress = new InetSocketAddress(host, mWorkerNetAddress.mSecondaryPort);
      LOG.info("Connecting " + (mIsLocal ? "local" : "remote") + " worker @ " + mWorkerAddress);

      mProtocol = new TBinaryProtocol(new TFramedTransport(new TSocket(host, port)));
      mClient = new WorkerService.Client(mProtocol);

      mHeartbeatExecutor = new WorkerClientHeartbeatExecutor(this);
      String threadName = "worker-heartbeat-" + mWorkerAddress;
      int interval = mTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS,
          Constants.SECOND_MS);
      mHeartbeat =
          mExecutorService.submit(new HeartbeatThread(threadName, mHeartbeatExecutor, interval));

      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return false;
      }
      mConnected = true;
    }

    return mConnected;
  }

  /**
   * Updates the user id of the client, starting a new session. The previous user's held
   * resources should have already been freed, and will be automatically freed after the timeout
   * is exceeded.
   *
   * @param newUserId the new id that represents the new session
   */
  public synchronized void createNewSession(long newUserId) {
    mUserId = newUserId;
  }

  /**
   * @return the address of the worker.
   */
  public synchronized InetSocketAddress getAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the address of the worker's data server.
   */
  public synchronized InetSocketAddress getDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  public synchronized long getUserId() {
    return mUserId;
  }

  /**
   * Get the user temporary folder in the under file system of the specified user.
   *
   * @return The user temporary folder in the under file system
   * @throws IOException
   */
  public synchronized String getUserUfsTempFolder() throws IOException {
    mustConnect();

    try {
      return mClient.getUserUfsTempFolder(mUserId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * @return true if it's connected to the worker, false otherwise.
   */
  public synchronized boolean isConnected() {
    return mConnected;
  }

  /**
   * @return true if the worker is local, false otherwise.
   */
  public synchronized boolean isLocal() {
    return mIsLocal;
  }

  /**
   * Lock the block, therefore, the worker will not evict the block from the memory until it is
   * unlocked.
   *
   * @param blockId The id of the block
   * @return the path of the block file locked
   * @throws IOException
   */
  public synchronized String lockBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.lockBlock(blockId, mUserId);
    } catch (FileDoesNotExistException e) {
      return null;
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Connect to the worker.
   *
   * @throws IOException
   */
  public synchronized void mustConnect() throws IOException {
    int tries = 0;
    while (tries ++ <= CONNECTION_RETRY_TIMES) {
      if (connect()) {
        return;
      }
    }
    throw new IOException("Failed to connect to the worker");
  }

  /**
   * Promote block back to the top StorageTier
   *
   * @param blockId The id of the block that will be promoted
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean promoteBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.promoteBlock(blockId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Get temporary path for the block from the worker
   *
   * @param blockId The id of the block
   * @param initialBytes The initial size bytes allocated for the block
   * @return the temporary path of the block
   * @throws IOException
   */
  public synchronized String requestBlockLocation(long blockId, long initialBytes)
      throws IOException {
    mustConnect();

    try {
      return mClient.requestBlockLocation(mUserId, blockId, initialBytes);
    } catch (OutOfSpaceException e) {
      throw new IOException(e);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Request space for some block from worker
   *
   * @param blockId The id of the block
   * @param requestBytes The requested space size, in bytes
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean requestSpace(long blockId, long requestBytes) throws IOException {
    mustConnect();

    try {
      return mClient.requestSpace(mUserId, blockId, requestBytes);
    } catch (OutOfSpaceException e) {
      return false;
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Unlock the block
   *
   * @param blockId The id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean unlockBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.unlockBlock(blockId, mUserId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Sends a user heartbeat to the worker. This renews the client's lease on resources such as
   * locks and temporary files and updates the worker's metrics.
   *
   * @throws IOException if an error occurs during the heartbeat
   */
  public synchronized void userHeartbeat() throws IOException {
    mustConnect();

    try {
      mClient.userHeartbeat(mUserId, mClientMetrics.getHeartbeatData());
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }
}
