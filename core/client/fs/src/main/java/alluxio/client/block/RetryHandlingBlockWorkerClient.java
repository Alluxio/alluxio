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
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.resource.LockBlockResource;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.CountingRetry;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.ThriftUtils;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * Note: Every client instance is associated with a session which is usually created for each block
 * stream. So be careful when reusing this client for multiple blocks.
 */
@ThreadSafe
public final class RetryHandlingBlockWorkerClient
    extends AbstractThriftClient<BlockWorkerClientService.Client> implements BlockWorkerClient {
  private static final Logger LOG = LoggerFactory.getLogger(RetryHandlingBlockWorkerClient.class);

  private static final ScheduledExecutorService HEARTBEAT_POOL = Executors.newScheduledThreadPool(
      Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_THREADS),
      ThreadFactoryUtils.build("block-worker-heartbeat-%d", true));
  private static final ExecutorService HEARTBEAT_CANCEL_POOL = Executors.newFixedThreadPool(5,
      ThreadFactoryUtils.build("block-worker-heartbeat-cancel-%d", true));
  private final BlockWorkerThriftClientPool mClientPool;
  private final BlockWorkerThriftClientPool mClientHeartbeatPool;
  // Tracks the number of active heartbeat close requests.
  private static final AtomicInteger NUM_ACTIVE_SESSIONS = new AtomicInteger(0);

  private final Long mSessionId;
  private final WorkerNetAddress mWorkerNetAddress;
  private final InetSocketAddress mRpcAddress;

  private ScheduledFuture<?> mHeartbeat;

  /**
   * Factory method for {@link RetryHandlingBlockWorkerClient}.
   *
   * @param clientPool the client pool
   * @param clientHeartbeatPool the client pool for heartbeat
   * @param workerNetAddress the worker address to connect to
   * @param sessionId the session id to use, this should be unique
   */
  protected static RetryHandlingBlockWorkerClient create(BlockWorkerThriftClientPool clientPool,
      BlockWorkerThriftClientPool clientHeartbeatPool, WorkerNetAddress workerNetAddress,
      Long sessionId) {
    RetryHandlingBlockWorkerClient client =
        new RetryHandlingBlockWorkerClient(clientPool, clientHeartbeatPool, workerNetAddress,
            sessionId);
    client.init();
    return client;
  }

  private RetryHandlingBlockWorkerClient(
      BlockWorkerThriftClientPool clientPool,
      BlockWorkerThriftClientPool clientHeartbeatPool,
      WorkerNetAddress workerNetAddress, final Long sessionId) {
    mClientPool = clientPool;
    mClientHeartbeatPool = clientHeartbeatPool;
    mWorkerNetAddress = Preconditions.checkNotNull(workerNetAddress, "workerNetAddress");
    mRpcAddress = NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress);
    mSessionId = sessionId;
  }

  private void init() {
    if (mSessionId != null) {
      // Register the session before any RPCs for this session start.
      ExponentialBackoffRetry retryPolicy =
          new ExponentialBackoffRetry(BASE_SLEEP_MS, MAX_SLEEP_MS, RPC_MAX_NUM_RETRY);
      sessionHeartbeat(retryPolicy);

      // The heartbeat is scheduled to run in a fixed rate. The heartbeat won't consume a thread
      // from the pool while it is not running.
      mHeartbeat = HEARTBEAT_POOL.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
              try {
                sessionHeartbeat(new CountingRetry(0));
              } catch (Exception e) {
                LOG.warn("Failed to heartbeat for session {}", mSessionId, e.getMessage());
              }
            }
          }, Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS),
          Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS), TimeUnit.MILLISECONDS);

      NUM_ACTIVE_SESSIONS.incrementAndGet();
    }
  }

  @Override
  public BlockWorkerClientService.Client acquireClient() {
    try {
      return mClientPool.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void releaseClient(BlockWorkerClientService.Client client) {
    mClientPool.release(client);
  }

  @Override
  public void close() {
    if (mHeartbeat != null) {
      HEARTBEAT_CANCEL_POOL.submit(new Runnable() {
        @Override
        public void run() {
          mHeartbeat.cancel(true);
          mHeartbeat = null;
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
  public void accessBlock(final long blockId) {
    retryRPC(new RpcCallable<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client) throws TException {
          client.accessBlock(blockId);
          return null;
      }
    });
  }

  @Override
  public void cacheBlock(final long blockId) {
    retryRPC(new RpcCallable<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client) throws TException {
        client.cacheBlock(getSessionId(), blockId);
        return null;
      }
    });
  }

  @Override
  public void cancelBlock(final long blockId) {
    retryRPC(new RpcCallable<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client) throws TException {
        client.cancelBlock(getSessionId(), blockId);
        return null;
      }
    });
  }

  @Override
  public long getSessionId() {
    Preconditions.checkNotNull(mSessionId, "Session ID is accessed when it is not supported");
    return mSessionId;
  }

  @Override
  public LockBlockResource lockBlock(final long blockId, final LockBlockOptions options) {
    LockBlockResult result = retryRPC(
        new RpcCallable<LockBlockResult, BlockWorkerClientService.Client>() {
          @Override
          public LockBlockResult call(BlockWorkerClientService.Client client) throws TException {
            return ThriftUtils
                .fromThrift(client.lockBlock(blockId, getSessionId(), options.toThrift()));
          }
        });
    return new LockBlockResource(this, result, blockId);
  }

  @Override
  public LockBlockResource lockUfsBlock(final long blockId, final LockBlockOptions options) {
    int retryInterval = Constants.SECOND_MS;
    RetryPolicy retryPolicy = new TimeoutRetry(Configuration
        .getLong(PropertyKey.USER_UFS_BLOCK_OPEN_TIMEOUT_MS), retryInterval);
    do {
      LockBlockResource resource = lockBlock(blockId, options);
      if (resource.getResult().getLockBlockStatus().ufsTokenNotAcquired()) {
        LOG.debug("Failed to acquire a UFS read token because of contention for block {} with "
            + "LockBlockOptions {}", blockId, options);
      } else {
        return resource;
      }
    } while (retryPolicy.attemptRetry());
    throw new UnavailableException(ExceptionMessage.UFS_BLOCK_ACCESS_TOKEN_UNAVAILABLE
        .getMessage(blockId, options.getUfsPath()));
  }

  @Override
  public boolean promoteBlock(final long blockId) {
    return retryRPC(
        new RpcCallable<Boolean, BlockWorkerClientService.Client>() {
          @Override
          public Boolean call(BlockWorkerClientService.Client client) throws TException {
            return client.promoteBlock(blockId);
          }
        });
  }

  @Override
  public void removeBlock(final long blockId) {
    retryRPC(new RpcCallable<Void, BlockWorkerClientService.Client>() {
      @Override
      public Void call(BlockWorkerClientService.Client client) throws TException {
        client.removeBlock(blockId);
        return null;
      }
    });
  }

  @Override
  public String requestBlockLocation(final long blockId, final long initialBytes,
      final int writeTier) {
    try {
      return retryRPC(
          new RpcCallable<String, BlockWorkerClientService.Client>() {
            @Override
            public String call(BlockWorkerClientService.Client client) throws TException {
              return client.requestBlockLocation(getSessionId(), blockId, initialBytes, writeTier);
            }
          });
    } catch (ResourceExhaustedException e) {
      throw new ResourceExhaustedException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mRpcAddress, blockId), e);
    }
  }

  @Override
  public boolean requestSpace(final long blockId, final long requestBytes) {
    boolean success = retryRPC(
        new RpcCallable<Boolean, BlockWorkerClientService.Client>() {
          @Override
          public Boolean call(BlockWorkerClientService.Client client) throws TException {
            return client.requestSpace(getSessionId(), blockId, requestBytes);
          }
        });
    if (!success) {
      throw new ResourceExhaustedException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, mRpcAddress, blockId));
    }
    return true;
  }

  @Override
  public boolean unlockBlock(final long blockId) {
    return retryRPC(new RpcCallable<Boolean, BlockWorkerClientService.Client>() {
      @Override
      public Boolean call(BlockWorkerClientService.Client client) throws TException {
          return client.unlockBlock(blockId, getSessionId());
      }
    });
  }

  /**
   * sessionHeartbeat is not retried because it is supposed to be called periodically.
   */
  @Override
  public void sessionHeartbeat(RetryPolicy retryPolicy) {
    TException exception;
    do {
      BlockWorkerClientService.Client client;
      try {
        client = mClientHeartbeatPool.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      try {
        client.sessionHeartbeat(mSessionId, null);
        Metrics.BLOCK_WORKER_HEATBEATS.inc();
        return;
      } catch (AlluxioTException e) {
        throw AlluxioStatusException.fromThrift(e);
      } catch (TException e) {
        client.getOutputProtocol().getTransport().close();
        exception = e;
        LOG.warn(e.getMessage());
      } finally {
        mClientHeartbeatPool.release(client);
      }
    } while (retryPolicy.attemptRetry());
    Preconditions.checkNotNull(exception);
    throw new UnavailableException(exception);
  }

  /**
   * Metrics related to the {@link RetryHandlingBlockWorkerClient}.
   */
  public static final class Metrics {
    private static final Counter BLOCK_WORKER_HEATBEATS =
        MetricsSystem.clientCounter("BlockWorkerHeartbeats");

    private Metrics() {} // prevent instantiation
  }
}
