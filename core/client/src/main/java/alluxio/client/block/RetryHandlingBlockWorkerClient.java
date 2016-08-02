/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.AbstractClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ClientMetrics;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The client talks to a block worker server. It keeps sending keep alive message to the worker
 * server.
 *
 * Since {@link alluxio.thrift.BlockWorkerClientService.Client} is not thread safe, this class
 * has to guarantee thread safety.
 */
@ThreadSafe
public final class RetryHandlingBlockWorkerClient extends AbstractClient
    implements BlockWorkerClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int CONNECTION_RETRY_TIMES = 5;

  private final boolean mIsLocal;

  private BlockWorkerClientService.Client mClient;
  private long mSessionId;
  // This is the address of the data server on the worker.
  private InetSocketAddress mWorkerDataServerAddress;
  private final WorkerNetAddress mWorkerNetAddress;
  private final ExecutorService mExecutorService;
  private final HeartbeatExecutor mHeartbeatExecutor;
  private Future<?> mHeartbeat;

  private final ClientMetrics mClientMetrics;

  /**
   * Creates a {@link RetryHandlingBlockWorkerClient}.
   *
   * @param workerNetAddress to worker's location
   * @param executorService the executor service
   * @param sessionId the id of the session
   * @param isLocal true if it is a local client, false otherwise
   * @param clientMetrics metrics of the client
   */
  public RetryHandlingBlockWorkerClient(WorkerNetAddress workerNetAddress,
      ExecutorService executorService, long sessionId, boolean isLocal,
      ClientMetrics clientMetrics) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress), "blockWorker");
    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress);
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mSessionId = sessionId;
    mIsLocal = isLocal;
    mClientMetrics = Preconditions.checkNotNull(clientMetrics);
    mHeartbeatExecutor = new BlockWorkerClientHeartbeatExecutor(this);
  }

  @Override
  public WorkerNetAddress getWorkerNetAddress() {
    return mWorkerNetAddress;
  }

  @Override
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

  @Override
  public synchronized boolean asyncCheckpoint(final long fileId) throws IOException,
      AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Boolean>() {
      @Override
      public Boolean call() throws AlluxioTException, TException {
        return mClient.asyncCheckpoint(fileId);
      }
    });
  }

  @Override
  public synchronized void cacheBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.cacheBlock(mSessionId, blockId);
        return null;
      }
    });
  }

  @Override
  public synchronized void cancelBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
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
  protected synchronized AlluxioService.Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION;
  }

  /**
   * Opens the connection to the worker. And start the heartbeat thread.
   *
   * @throws IOException if a non-Alluxio exception occurs
   */
  private synchronized void connectOperation() throws IOException {
    if (!mConnected) {
      LOG.info("Connecting to {} worker @ {}", (mIsLocal ? "local" : "remote"), mAddress);

      TProtocol binaryProtocol =
          new TBinaryProtocol(mTransportProvider.getClientTransport(mAddress));
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
        final int interval = Configuration.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS);
        mHeartbeat =
            mExecutorService.submit(new HeartbeatThread(HeartbeatContext.WORKER_CLIENT,
                mHeartbeatExecutor, interval));
      }
    }
  }

  @Override
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

  @Override
  public synchronized InetSocketAddress getDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  @Override
  public synchronized long getSessionId() {
    return mSessionId;
  }

  @Override
  public synchronized boolean isLocal() {
    return mIsLocal;
  }

  @Override
  public synchronized LockBlockResult lockBlock(final long blockId) throws IOException {
    // TODO(jiri) Would be nice to have a helper method to execute this try-catch logic
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<LockBlockResult>() {
        @Override
        public LockBlockResult call() throws AlluxioTException, TException {
          return ThriftUtils.fromThrift(mClient.lockBlock(blockId, mSessionId));
        }
      });
    } catch (AlluxioException e) {
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
   * @throws IOException if a non-Alluxio exception occurs
   */
  // TODO(jiezhou): Consider merging the connect logic in this method into the super class.
  @Override
  public synchronized void connect() throws IOException {
    int tries = 0;
    while (tries++ <= CONNECTION_RETRY_TIMES) {
      connectOperation();
      if (isConnected()) {
        return;
      }
    }
    throw new IOException("Failed to connect to the worker");
  }

  @Override
  public synchronized boolean promoteBlock(final long blockId) throws IOException,
      AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Boolean>() {
      @Override
      public Boolean call() throws AlluxioTException, TException {
        return mClient.promoteBlock(blockId);
      }
    });
  }

  @Override
  public synchronized String requestBlockLocation(final long blockId, final long initialBytes)
      throws IOException {
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<String>() {
        @Override
        public String call() throws AlluxioTException, TException {
          return mClient.requestBlockLocation(mSessionId, blockId, initialBytes);
        }
      });
    } catch (AlluxioException e) {
      if (e instanceof WorkerOutOfSpaceException) {
        throw new IOException("Failed to request " + initialBytes, e);
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  public synchronized boolean requestSpace(final long blockId, final long requestBytes)
      throws IOException {
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<Boolean>() {
        @Override
        public Boolean call() throws AlluxioTException, TException {
          return mClient.requestSpace(mSessionId, blockId, requestBytes);
        }
      });
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized boolean unlockBlock(final long blockId) throws ConnectionFailedException,
      IOException {
    return retryRPC(new RpcCallable<Boolean>() {
      @Override
      public Boolean call() throws TException {
        return mClient.unlockBlock(blockId, mSessionId);
      }
    });
  }

  @Override
  public synchronized void sessionHeartbeat() throws ConnectionFailedException, IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call() throws TException {
        mClient.sessionHeartbeat(mSessionId, mClientMetrics.getHeartbeatData());
        return null;
      }
    });
  }

  @Override
  public synchronized void periodicHeartbeat() {
    if (mClosed) {
      return;
    }
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

  @Override
  public ClientMetrics getClientMetrics() {
    return mClientMetrics;
  }
}
