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

import alluxio.AbstractThriftClient;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
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

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
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
public final class RetryHandlingBlockWorkerClient extends AbstractThriftClient
    implements BlockWorkerClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final boolean mIsLocal;

  private final long mSessionId;
  // This is the address of the data server on the worker.
  private final InetSocketAddress mWorkerDataServerAddress;
  private final WorkerNetAddress mWorkerNetAddress;
  private final InetSocketAddress mRpcAddress;
  private final ExecutorService mExecutorService;
  private final HeartbeatExecutor mHeartbeatExecutor;
  private final Future<?> mHeartbeat;

  /**
   * Creates a {@link RetryHandlingBlockWorkerClient}.
   *
   * @param workerNetAddress to worker's location
   * @param executorService the executor service
   * @param sessionId the id of the session
   * @param isLocal true if it is a local client, false otherwise
   */
  public RetryHandlingBlockWorkerClient(WorkerNetAddress workerNetAddress,
      ExecutorService executorService, long sessionId, boolean isLocal) throws IOException {
    mRpcAddress = NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress);

    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress);
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mSessionId = sessionId;
    mIsLocal = isLocal;
    mHeartbeatExecutor = new BlockWorkerClientHeartbeatExecutor(this);
    try {
      sessionHeartbeat();
    } catch (IOException e) {
      LOG.error("Failed to send initial heartbeat to register a session with the worker.", e);
      throw e;
    }
    mHeartbeat = mExecutorService.submit(
        new HeartbeatThread(HeartbeatContext.WORKER_CLIENT, mHeartbeatExecutor,
            Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS)));
  }

  @Override
  public AlluxioService.Client acquireClient() throws IOException {
    return BlockStoreContext.acquireBlockWorkerThriftClient(mRpcAddress);
  }

  @Override
  public void releaseClient(AlluxioService.Client client) {
    BlockStoreContext
        .releaseBlockWorkerThriftClient(mRpcAddress, ((BlockWorkerClientService.Client) client));
  }

  @Override
  public void close() {
    if (mHeartbeat != null) {
      mHeartbeat.cancel(true);
    }
  }

  @Override
  public WorkerNetAddress getWorkerNetAddress() {
    return mWorkerNetAddress;
  }

  @Override
  public void accessBlock(final long blockId) throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call(AlluxioService.Client client) throws TException {
        ((BlockWorkerClientService.Client) client).accessBlock(blockId);
        return null;
      }
    });
  }

  @Override
  public void cacheBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call(AlluxioService.Client client) throws AlluxioTException, TException {
        ((BlockWorkerClientService.Client) client).cacheBlock(mSessionId, blockId);
        return null;
      }
    });
  }

  @Override
  public void cancelBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call(AlluxioService.Client client) throws AlluxioTException, TException {
        ((BlockWorkerClientService.Client) client).cancelBlock(mSessionId, blockId);
        return null;
      }
    });
  }

  @Override
  public InetSocketAddress getDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  @Override
  public long getSessionId() {
    return mSessionId;
  }

  @Override
  public boolean isLocal() {
    return mIsLocal;
  }

  @Override
  public LockBlockResult lockBlock(final long blockId) throws IOException {
    // TODO(jiri) Would be nice to have a helper method to execute this try-catch logic
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<LockBlockResult>() {
        @Override
        public LockBlockResult call(AlluxioService.Client client)
            throws AlluxioTException, TException {
          return ThriftUtils.fromThrift(
              ((BlockWorkerClientService.Client) client).lockBlock(blockId, mSessionId));
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

  @Override
  public boolean promoteBlock(final long blockId) throws IOException, AlluxioException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Boolean>() {
      @Override
      public Boolean call(AlluxioService.Client client) throws AlluxioTException, TException {
        return ((BlockWorkerClientService.Client) client).promoteBlock(blockId);
      }
    });
  }

  @Override
  public synchronized String requestBlockLocation(final long blockId, final long initialBytes)
      throws IOException {
    try {
      return retryRPC(new RpcCallableThrowsAlluxioTException<String>() {
        @Override
        public String call(AlluxioService.Client client) throws AlluxioTException, TException {
          return ((BlockWorkerClientService.Client) client)
              .requestBlockLocation(mSessionId, blockId, initialBytes);
        }
      });
    } catch (WorkerOutOfSpaceException e) {
      throw new IOException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mRpcAddress, blockId));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized boolean requestSpace(final long blockId, final long requestBytes)
      throws IOException {
    try {
      boolean success = retryRPC(new RpcCallableThrowsAlluxioTException<Boolean>() {
        @Override
        public Boolean call(AlluxioService.Client client) throws AlluxioTException, TException {
          return ((BlockWorkerClientService.Client) client)
              .requestSpace(mSessionId, blockId, requestBytes);
        }
      });
      if (!success) {
        throw new IOException(ExceptionMessage.CANNOT_REQUEST_SPACE
            .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mRpcAddress, blockId));
      }
      return true;
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean unlockBlock(final long blockId) throws IOException {
    return retryRPC(new RpcCallable<Boolean>() {
      @Override
      public Boolean call(AlluxioService.Client client) throws TException {
        return ((BlockWorkerClientService.Client) client).unlockBlock(blockId, mSessionId);
      }
    });
  }

  @Override
  public void sessionHeartbeat() throws IOException {
    retryRPC(new RpcCallable<Void>() {
      @Override
      public Void call(AlluxioService.Client client) throws TException {
        ((BlockWorkerClientService.Client) client).sessionHeartbeat(mSessionId, null);
        return null;
      }
    });
  }

  @Override
  public void periodicHeartbeat() {
    try {
      sessionHeartbeat();
    } catch (Exception e) {
      LOG.error("Periodic heartbeat failed, cleaning up.", e);
    }
  }
}
