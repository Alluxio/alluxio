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

package tachyon.client.block;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.ClientBase;
import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.TachyonException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.WorkerOutOfSpaceException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatExecutor;
import tachyon.heartbeat.HeartbeatThread;
import tachyon.security.authentication.AuthenticationUtils;
import tachyon.thrift.BlockWorkerClientService;
import tachyon.thrift.LockBlockResult;
import tachyon.thrift.TachyonService;
import tachyon.thrift.TachyonTException;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.ClientMetrics;
import tachyon.worker.NetAddress;

/**
 * The client talks to a block worker server. It keeps sending keep alive message to the worker
 * server.
 *
 * Since {@link BlockWorkerClientService.Client} is not thread safe, this class has to guarantee
 * thread safety.
 */
@ThreadSafe
public final class BlockWorkerClient extends ClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int CONNECTION_RETRY_TIMES = 5;

  private final boolean mIsLocal;

  private BlockWorkerClientService.Client mClient;
  private long mSessionId;
  // This is the address of the data server on the worker.
  private InetSocketAddress mWorkerDataServerAddress;
  private final ExecutorService mExecutorService;
  private final HeartbeatExecutor mHeartbeatExecutor;
  private Future<?> mHeartbeat;

  private final ClientMetrics mClientMetrics;

  /**
   * Creates a {@link BlockWorkerClient}.
   *
   * @param workerNetAddress to worker's location
   * @param executorService the executor service
   * @param conf Tachyon configuration
   * @param sessionId the id of the session
   * @param isLocal true if it is a local client, false otherwise
   * @param clientMetrics metrics of the client
   */
  public BlockWorkerClient(NetAddress workerNetAddress, ExecutorService executorService,
      TachyonConf conf, long sessionId, boolean isLocal, ClientMetrics clientMetrics) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress), conf, "blockWorker");
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mSessionId = sessionId;
    mIsLocal = isLocal;
    mClientMetrics = Preconditions.checkNotNull(clientMetrics);
    mHeartbeatExecutor = new BlockWorkerClientHeartbeatExecutor(this);
  }

  /**
   * Updates the latest block access time on the worker.
   *
   * @param blockId The id of the block
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized void accessBlock(final long blockId) throws ConnectionFailedException,
      IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.accessBlock(blockId);
        return null;
      }
    });
  }

  /**
   * Notifies the worker to checkpoint the file asynchronously.
   *
   * @param fileId The id of the file
   * @return true if success, false otherwise
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean asyncCheckpoint(final long fileId) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.asyncCheckpoint(fileId);
      }
    });
  }

  /**
   * Notifies the worker the block is cached.
   *
   * @param blockId The id of the block
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void cacheBlock(final long blockId) throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.cacheBlock(mSessionId, blockId);
        return null;
      }
    });
  }

  /**
   * Notifies worker that the block has been cancelled.
   *
   * @param blockId The Id of the block to be cancelled
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized void cancelBlock(final long blockId) throws IOException, TachyonException {
    retryRPC(new RpcCallableThrowsTachyonTException<Void>() {
      @Override
      public Void call() throws TachyonTException, TException {
        mClient.cancelBlock(mSessionId, blockId);
        return null;
      }
    });
  }

  @Override
  protected synchronized void beforeDisconnect() {
    // Heartbeat to send the client metrics.
    if (mHeartbeatExecutor != null) {
      mHeartbeatExecutor.heartbeat();
    }
  }

  @Override
  protected synchronized void afterDisconnect() {
    if (mHeartbeat != null) {
      mHeartbeat.cancel(true);
    }
  }

  @Override
  protected synchronized TachyonService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.BLOCK_WORKER_SERVICE_VERSION;
  }

  /**
   * Opens the connection to the worker. And start the heartbeat thread.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  private synchronized void connectOperation() throws IOException {
    if (!mConnected) {
      LOG.info("Connecting to {} worker @ {}", (mIsLocal ? "local" : "remote"), mAddress);

      TProtocol binaryProtocol =
          new TBinaryProtocol(AuthenticationUtils.getClientTransport(mTachyonConf, mAddress));
      mProtocol = new TMultiplexedProtocol(binaryProtocol, getServiceName());
      mClient = new BlockWorkerClientService.Client(mProtocol);

      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return;
      }
      mConnected = true;

      // only start the heartbeat thread if the connection is successful and if there is not
      // another heartbeat thread running
      if (mHeartbeat == null || mHeartbeat.isCancelled() || mHeartbeat.isDone()) {
        final int interval = mTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS);
        mHeartbeat =
            mExecutorService.submit(new HeartbeatThread(HeartbeatContext.WORKER_CLIENT,
                mHeartbeatExecutor, interval));
      }
    }
  }

  /**
   * Updates the session id of the client, starting a new session. The previous session's held
   * resources should have already been freed, and will be automatically freed after the timeout is
   * exceeded.
   *
   * @param newSessionId the new id that represents the new session
   */
  public synchronized void createNewSession(long newSessionId) {
    mSessionId = newSessionId;
  }

  /**
   * @return the address of the worker
   */
  @Override
  public synchronized InetSocketAddress getAddress() {
    return mAddress;
  }

  /**
   * @return the address of the worker's data server
   */
  public synchronized InetSocketAddress getDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  /**
   * @return the id of the session
   */
  public synchronized long getSessionId() {
    return mSessionId;
  }

  /**
   * @return true if the worker is local, false otherwise
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
   * @throws IOException if a non-Tachyon exception occurs
   */
  public synchronized LockBlockResult lockBlock(final long blockId) throws IOException {
    // TODO(jiri) Would be nice to have a helper method to execute this try-catch logic
    try {
      return retryRPC(new RpcCallableThrowsTachyonTException<LockBlockResult>() {
        @Override
        public LockBlockResult call() throws TachyonTException, TException {
          return mClient.lockBlock(blockId, mSessionId);
        }
      });
    } catch (TachyonException e) {
      if (e instanceof FileDoesNotExistException) {
        return null;
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Connects to the worker.
   *
   * @throws IOException if a non-Tachyon exception occurs
   */
  // TODO(jiezhou): Consider merging the connect logic in this method into the super class.
  @Override
  public synchronized void connect() throws IOException {
    int tries = 0;
    while (tries ++ <= CONNECTION_RETRY_TIMES) {
      connectOperation();
      if (isConnected()) {
        return;
      }
    }
    throw new IOException("Failed to connect to the worker");
  }

  /**
   * Promotes block back to the top StorageTier.
   *
   * @param blockId The id of the block that will be promoted
   * @return true if succeed, false otherwise
   * @throws IOException if an I/O error occurs
   * @throws TachyonException if a Tachyon error occurs
   */
  public synchronized boolean promoteBlock(final long blockId) throws IOException,
      TachyonException {
    return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
      @Override
      public Boolean call() throws TachyonTException, TException {
        return mClient.promoteBlock(blockId);
      }
    });
  }

  /**
   * Gets temporary path for the block from the worker.
   *
   * @param blockId The id of the block
   * @param initialBytes The initial size bytes allocated for the block
   * @return the temporary path of the block
   * @throws IOException if a non-Tachyon exception occurs
   */
  public synchronized String requestBlockLocation(final long blockId, final long initialBytes)
      throws IOException {
    try {
      return retryRPC(new RpcCallableThrowsTachyonTException<String>() {
        @Override
        public String call() throws TachyonTException, TException {
          return mClient.requestBlockLocation(mSessionId, blockId, initialBytes);
        }
      });
    } catch (TachyonException e) {
      if (e instanceof WorkerOutOfSpaceException) {
        throw new IOException("Failed to request " + initialBytes, e);
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Requests space for some block from worker.
   *
   * @param blockId The id of the block
   * @param requestBytes The requested space size, in bytes
   * @return true if success, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   */
  public synchronized boolean requestSpace(final long blockId, final long requestBytes)
      throws IOException {
    try {
      return retryRPC(new RpcCallableThrowsTachyonTException<Boolean>() {
        @Override
        public Boolean call() throws TachyonTException, TException {
          return mClient.requestSpace(mSessionId, blockId, requestBytes);
        }
      });
    } catch (TachyonException e) {
      if (e instanceof WorkerOutOfSpaceException) {
        return false;
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Unlocks the block.
   *
   * @param blockId The id of the block
   * @return true if success, false otherwise
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean unlockBlock(final long blockId) throws ConnectionFailedException,
      IOException {
    return retryRPC(new RpcCallable<Boolean>() {
      @Override
      public Boolean call() throws TException {
        return mClient.unlockBlock(blockId, mSessionId);
      }
    });
  }

  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * locks and temporary files and updates the worker's metrics.
   *
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  public synchronized void sessionHeartbeat() throws ConnectionFailedException, IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.sessionHeartbeat(mSessionId, mClientMetrics.getHeartbeatData());
        return null;
      }
    });
  }

  /**
   * Called only by {@link BlockWorkerClientHeartbeatExecutor}, encapsulates
   * {@link #sessionHeartbeat()} in order to cancel and cleanup the heartbeating thread in case of
   * failures.
   */
  public synchronized void periodicHeartbeat() {
    try {
      sessionHeartbeat();
    } catch (Exception e) {
      LOG.error("Periodic heartbeat failed, cleaning up.", e);
      if (mHeartbeat != null) {
        mHeartbeat.cancel(true);
        mHeartbeat = null;
      }
    }
  }
}
