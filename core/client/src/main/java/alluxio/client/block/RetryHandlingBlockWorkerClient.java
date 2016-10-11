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
import alluxio.metrics.MetricsSystem;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The client talks to a block worker server. It keeps sending keep alive message to the worker
 * server.
 */
@ThreadSafe
public final class RetryHandlingBlockWorkerClient
    extends AbstractThriftClient<BlockWorkerClientService.Client> implements BlockWorkerClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final ScheduledExecutorService HEARTBEAT_POOL = Executors.newScheduledThreadPool(
      Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_THREADS),
      ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
  private static final ExecutorService HEARTBEAT_CANCEL_POOL = Executors.newFixedThreadPool(5,
      ThreadFactoryUtils.build("block-worker-heartbeat-cancel-%d", true));

  // Tracks the number of active heartbeat close requests.
  private static final AtomicInteger NUM_ACTIVE_SESSIONS = new AtomicInteger(0);

  private final Long mSessionId;
  // This is the address of the data server on the worker.
  private final InetSocketAddress mWorkerDataServerAddress;
  private final WorkerNetAddress mWorkerNetAddress;
  private final InetSocketAddress mRpcAddress;

  private ScheduledFuture<?> mHeartbeat = null;

  /**
   * Creates a {@link RetryHandlingBlockWorkerClient}. Set sessionId to null if no session Id is
   * required when using this client. For example, if you only call RPCs like promote, a session
   * Id is not required.
   *
   * @param workerNetAddress to worker's location
   * @param sessionId the id of the session
   * @throws IOException if it fails to register the session with the worker specified
   */
  public RetryHandlingBlockWorkerClient(WorkerNetAddress workerNetAddress, final Long sessionId)
      throws IOException {
    mRpcAddress = NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress);

    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress);
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mSessionId = sessionId;
    if (sessionId != null) {
      // Register the session before any RPCs for this session start.
      try {
        sessionHeartbeat();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }

      // The heartbeat is scheduled to run in a fixed rate. The heartbeat won't consume a thread
      // from the pool while it is not running.
      mHeartbeat = HEARTBEAT_POOL.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
              try {
                sessionHeartbeat();
              } catch (InterruptedException e) {
                // Do nothing.
              } catch (Exception e) {
                LOG.error("Failed to heartbeat for session " + sessionId, e);
              }
            }
          }, Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS),
          Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS), TimeUnit.MILLISECONDS);

      NUM_ACTIVE_SESSIONS.incrementAndGet();
    }
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
      HEARTBEAT_CANCEL_POOL.submit(new Runnable() {
        @Override
        public void run() {
          mHeartbeat.cancel(true);
          NUM_ACTIVE_SESSIONS.decrementAndGet();
        }
      });
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
        client.cacheBlock(getSessionId(), blockId);
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
        client.cancelBlock(getSessionId(), blockId);
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
    Preconditions.checkNotNull(mSessionId, "SessionId is accessed when it is not supported");
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
              return ThriftUtils.fromThrift(client.lockBlock(blockId, getSessionId()));
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
              return client.requestBlockLocation(getSessionId(), blockId, initialBytes);
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
              return client.requestSpace(getSessionId(), blockId, requestBytes);
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
          return client.unlockBlock(blockId, getSessionId());
      }
    });
  }

  /**
   * sessionHeartbeat is not retried because it is supposed to be called periodically.
   *
   * @throws IOException if it fails to heartbeat
   * @throws InterruptedException if heartbeat is interrupted
   */
  @Override
  public void sessionHeartbeat() throws IOException, InterruptedException {
    BlockWorkerClientService.Client client =
        BlockStoreContext.acquireBlockWorkerThriftClientHeartbeat(mRpcAddress);
    try {
      client.sessionHeartbeat(getSessionId(), null);
    } catch (AlluxioTException e) {
      throw Throwables.propagate(e);
    } catch (ThriftIOException e) {
      throw new IOException(e);
    } catch (TException e) {
      client.getOutputProtocol().getTransport().close();
      throw new IOException(e);
    } finally {
      BlockStoreContext.releaseBlockWorkerThriftClientHeartbeat(mRpcAddress, client);
    }
    Metrics.BLOCK_WORKER_HEATBEATS.inc();
  }

  /**
   * Metrics related to the {@link RetryHandlingBlockWorkerClient}.
   */
  public static final class Metrics {
    private static final Counter BLOCK_WORKER_HEATBEATS =
        MetricsSystem.clientCounter("BlockWorkerHeartbeats");

    private Metrics() {
    } // prevent instantiation
  }
}
