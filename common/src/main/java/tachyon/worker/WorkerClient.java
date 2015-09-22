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

import com.google.common.base.Preconditions;

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

/**
 * The client talks to a worker server. It keeps sending keep alive message to the worker server.
 *
 * Since WorkerService.Client is not thread safe, this class has to guarantee thread safety.
 */
public final class WorkerClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int CONNECTION_RETRY_TIMES = 5;

  private final NetAddress mWorkerNetAddress;
  private final boolean mIsLocal;

  private WorkerService.Client mClient;
  private TProtocol mProtocol;
  private long mSessionId;
  private InetSocketAddress mWorkerAddress;
  // This is the address of the data server on the worker.
  private InetSocketAddress mWorkerDataServerAddress;
  // TODO(hy): This boolean indicates whether or not the client is connected to the worker. However,
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
   * Creates a WorkerClient.
   *
   * @param workerNetAddress to worker's location
   * @param executorService the executor service
   * @param conf Tachyon configuration
   * @param clientMetrics metrics of the lcient.
   */
  public WorkerClient(NetAddress workerNetAddress, ExecutorService executorService,
      TachyonConf conf, long sessionId, boolean isLocal, ClientMetrics clientMetrics) {
    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mTachyonConf = Preconditions.checkNotNull(conf);
    mSessionId = sessionId;
    mIsLocal = isLocal;
    mClientMetrics = Preconditions.checkNotNull(clientMetrics);
  }

  /**
   * Updates the latest block access time on the worker.
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
   * Notifies the worker that the checkpoint file of the file has been added.
   *
   * @param fileId The id of the checkpointed file
   * @throws IOException
   */
  public synchronized void addCheckpoint(long fileId, long nonce) throws IOException {
    mustConnect();

    try {
      mClient.addCheckpoint(fileId, nonce);
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
   * Notifies the worker to checkpoint the file asynchronously.
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
   * Notifies the worker the block is cached.
   *
   * @param blockId The id of the block
   * @throws IOException
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.cacheBlock(mSessionId, blockId);
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
   * Notifies worker that the block has been cancelled
   *
   * @param blockId The Id of the block to be cancelled
   * @throws IOException
   */
  public synchronized void cancelBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.cancelBlock(mSessionId, blockId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Closes the connection to worker. Shutdown the heartbeat thread.
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
   * Opens the connection to the worker. And start the heartbeat thread.
   *
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  private synchronized boolean connect() throws IOException {
    if (!mConnected) {
      String host = NetworkAddressUtils.getFqdnHost(mWorkerNetAddress);
      int port = mWorkerNetAddress.rpcPort;
      mWorkerAddress = new InetSocketAddress(host, port);
      mWorkerDataServerAddress = new InetSocketAddress(host, mWorkerNetAddress.dataPort);
      LOG.info("Connecting " + (mIsLocal ? "local" : "remote") + " worker @ " + mWorkerAddress);

      mProtocol = new TBinaryProtocol(new TFramedTransport(new TSocket(host, port)));
      mClient = new WorkerService.Client(mProtocol);

      mHeartbeatExecutor = new WorkerClientHeartbeatExecutor(this);
      String threadName = "worker-heartbeat-" + mWorkerAddress;
      int interval = mTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS);
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
   * Updates the session id of the client, starting a new session. The previous session's held
   * resources should have already been freed, and will be automatically freed after the timeout
   * is exceeded.
   *
   * @param newSessionId the new id that represents the new session
   */
  public synchronized void createNewSession(long newSessionId) {
    mSessionId = newSessionId;
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

  public synchronized long getSessionId() {
    return mSessionId;
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
   * Locks the block, therefore, the worker will not evict the block from the memory until it is
   * unlocked.
   *
   * @param blockId The id of the block
   * @return the path of the block file locked
   * @throws IOException
   */
  public synchronized String lockBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.lockBlock(blockId, mSessionId);
    } catch (FileDoesNotExistException e) {
      return null;
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Connects to the worker.
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
   * Promotes block back to the top StorageTier
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
   * Gets temporary path for the block from the worker
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
      return mClient.requestBlockLocation(mSessionId, blockId, initialBytes);
    } catch (OutOfSpaceException e) {
      throw new IOException("Failed to request " + initialBytes, e);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Requests space for some block from worker
   *
   * @param blockId The id of the block
   * @param requestBytes The requested space size, in bytes
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean requestSpace(long blockId, long requestBytes) throws IOException {
    mustConnect();

    try {
      return mClient.requestSpace(mSessionId, blockId, requestBytes);
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
   * Unlocks the block
   *
   * @param blockId The id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean unlockBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.unlockBlock(blockId, mSessionId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * locks and temporary files and updates the worker's metrics.
   *
   * @throws IOException if an error occurs during the heartbeat
   */
  public synchronized void sessionHeartbeat() throws IOException {
    mustConnect();

    try {
      mClient.sessionHeartbeat(mSessionId, mClientMetrics.getHeartbeatData());
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }
}
