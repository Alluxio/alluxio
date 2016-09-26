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
public final class RetryHandlingBlockWorkerClient
    extends AbstractThriftClient<BlockWorkerClientService.Client> implements BlockWorkerClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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
   * @throws IOException if it fails to register the session with the worker specified
   */
  public RetryHandlingBlockWorkerClient(WorkerNetAddress workerNetAddress,
      ExecutorService executorService, long sessionId) throws IOException {
    mRpcAddress = NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress);

    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress);
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mSessionId = sessionId;
    mHeartbeatExecutor = new BlockWorkerClientHeartbeatExecutor(this);
    sessionHeartbeat();
    mHeartbeat = mExecutorService.submit(
        new HeartbeatThread(HeartbeatContext.WORKER_CLIENT, mHeartbeatExecutor, Configuration
            .getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS)));
  }

  @Override
  public BlockWorkerClientService.Client acquireClient() throws IOException {
    return BlockStoreContext.acquireBlockWorkerThriftClient(mRpcAddress);
  }

  @Override
  public void releaseClient(BlockWorkerClientService.Client client) {
    BlockStoreContext.releaseBlockWorkerThriftClient(mRpcAddress, client);
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
    retryRPC(new RpcCallable<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client) throws TException {
          client.accessBlock(blockId);
          return null;
      }
    });
  }

  @Override
  public void cacheBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.cacheBlock(mSessionId, blockId);
        return null;
      }
    });
  }

  @Override
  public void cancelBlock(final long blockId) throws IOException, AlluxioException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.cancelBlock(mSessionId, blockId);
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
  public LockBlockResult lockBlock(final long blockId) throws IOException {
    // TODO(jiri) Would be nice to have a helper method to execute this try-catch logic
    try {
      return retryRPC(
          new RpcCallableThrowsAlluxioTException<LockBlockResult, BlockWorkerClientService
              .Client>() {
            @Override
            public LockBlockResult call(BlockWorkerClientService.Client client)
                throws AlluxioTException, TException {
              return ThriftUtils.fromThrift(client.lockBlock(blockId, mSessionId));
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
    return retryRPC(
        new RpcCallableThrowsAlluxioTException<Boolean, BlockWorkerClientService.Client>() {
          @Override
          public Boolean call(BlockWorkerClientService.Client client)
              throws AlluxioTException, TException {
            return client.promoteBlock(blockId);
          }
        });
  }

  @Override
  public String requestBlockLocation(final long blockId, final long initialBytes)
      throws IOException {
    try {
      return retryRPC(
          new RpcCallableThrowsAlluxioTException<String, BlockWorkerClientService.Client>() {
            @Override
            public String call(BlockWorkerClientService.Client client)
                throws AlluxioTException, TException {
              return client.requestBlockLocation(mSessionId, blockId, initialBytes);
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
  public boolean requestSpace(final long blockId, final long requestBytes) throws IOException {
    try {
      boolean success = retryRPC(
          new RpcCallableThrowsAlluxioTException<Boolean, BlockWorkerClientService.Client>() {
            @Override
            public Boolean call(BlockWorkerClientService.Client client)
                throws AlluxioTException, TException {
              return client.requestSpace(mSessionId, blockId, requestBytes);
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
    return retryRPC(new RpcCallable<Boolean, BlockWorkerClientService.Client>() {
      @Override
      public Boolean call(BlockWorkerClientService.Client client) throws TException {
          return client.unlockBlock(blockId, mSessionId);
      }
    });
  }

  @Override
  public void sessionHeartbeat() throws IOException {
    retryRPC(new RpcCallable<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client) throws TException {
          client.sessionHeartbeat(mSessionId, null);
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
