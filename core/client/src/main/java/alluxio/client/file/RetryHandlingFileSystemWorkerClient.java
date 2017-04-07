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

package alluxio.client.file;

import alluxio.AbstractThriftClient;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.options.CancelUfsFileOptions;
import alluxio.client.file.options.CloseUfsFileOptions;
import alluxio.client.file.options.CompleteUfsFileOptions;
import alluxio.client.file.options.CreateUfsFileOptions;
import alluxio.client.file.options.OpenUfsFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.CountingRetry;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
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
 * Client for talking to a file system worker server. It keeps sending keep alive messages to the
 * worker server to preserve its state.
 */
// TODO(calvin): Session logic can be abstracted
@ThreadSafe
public final class RetryHandlingFileSystemWorkerClient
    extends AbstractThriftClient<FileSystemWorkerClientService.Client>
    implements FileSystemWorkerClient {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWorkerClient.class);
  private static final ScheduledExecutorService HEARTBEAT_POOL = Executors.newScheduledThreadPool(
      Configuration.getInt(PropertyKey.USER_FILE_WORKER_CLIENT_THREADS),
      ThreadFactoryUtils.build("file-worker-heartbeat-%d", true));
  private static final ExecutorService HEARTBEAT_CANCEL_POOL = Executors.newFixedThreadPool(5,
      ThreadFactoryUtils.build("file-worker-heartbeat-cancel-%d", true));

  // Tracks the number of active heartbeats.
  private static final AtomicInteger NUM_ACTIVE_SESSIONS = new AtomicInteger(0);

  private final FileSystemWorkerThriftClientPool mClientPool;
  private final FileSystemWorkerThriftClientPool mClientHeartbeatPool;

  /** The current session id, managed by the caller. */
  private final long mSessionId;

  /** Address of the data server on the worker. */
  private final InetSocketAddress mWorkerDataServerAddress;

  private ScheduledFuture<?> mHeartbeat;

  /**
   * Factory method for {@link RetryHandlingFileSystemWorkerClient}.
   *
   * @param clientPool the client pool
   * @param clientHeartbeatPool the client pool for heartbeat
   * @param workerNetAddress the worker address to connect to
   * @param sessionId the session id to use, this should be unique
   * @throws IOException if it fails to register the session with the worker specified
   */
  protected static RetryHandlingFileSystemWorkerClient create(
      FileSystemWorkerThriftClientPool clientPool,
      FileSystemWorkerThriftClientPool clientHeartbeatPool, WorkerNetAddress workerNetAddress,
      long sessionId) throws IOException {
    RetryHandlingFileSystemWorkerClient client =
        new RetryHandlingFileSystemWorkerClient(clientPool, clientHeartbeatPool, workerNetAddress,
            sessionId);
    client.init();
    return client;
  }

  private RetryHandlingFileSystemWorkerClient(FileSystemWorkerThriftClientPool clientPool,
      FileSystemWorkerThriftClientPool clientHeartbeatPool, WorkerNetAddress workerNetAddress,
      long sessionId) {
    mClientPool = clientPool;
    mClientHeartbeatPool = clientHeartbeatPool;
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mSessionId = sessionId;
  }

  private void init() throws IOException {
    // Register the session before any RPCs for this session start.
    ExponentialBackoffRetry retryPolicy =
        new ExponentialBackoffRetry(BASE_SLEEP_MS, MAX_SLEEP_MS, RPC_MAX_NUM_RETRY);
    try {
      sessionHeartbeat(retryPolicy);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // The heartbeat is scheduled to run in a fixed rate. The heartbeat won't consume a thread
    // from the pool while it is not running.
    mHeartbeat = HEARTBEAT_POOL.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            try {
              sessionHeartbeat(new CountingRetry(0));
            } catch (InterruptedException e) {
              // do nothing
            } catch (Exception e) {
              LOG.warn("Failed to heartbeat for session {}", mSessionId, e.getMessage());
            }
          }
        }, Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS),
        Configuration.getInt(PropertyKey.USER_HEARTBEAT_INTERVAL_MS), TimeUnit.MILLISECONDS);

    NUM_ACTIVE_SESSIONS.incrementAndGet();
  }

  @Override
  public FileSystemWorkerClientService.Client acquireClient() throws IOException {
    try {
      return mClientPool.acquire();
    } catch (InterruptedException e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void releaseClient(FileSystemWorkerClientService.Client client) {
    mClientPool.release(client);
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
  public void cancelUfsFile(final long tempUfsFileId, final CancelUfsFileOptions options)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, FileSystemWorkerClientService.Client>() {
      @Override
      public Void call(FileSystemWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.cancelUfsFile(mSessionId, tempUfsFileId, options.toThrift());
        return null;
      }
    });
  }

  @Override
  public void closeUfsFile(final long tempUfsFileId, final CloseUfsFileOptions options)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void, FileSystemWorkerClientService.Client>() {
      @Override
      public Void call(FileSystemWorkerClientService.Client client)
          throws AlluxioTException, TException {
        client.closeUfsFile(mSessionId, tempUfsFileId, options.toThrift());
        return null;
      }
    });
  }

  @Override
  public long completeUfsFile(final long tempUfsFileId, final CompleteUfsFileOptions options)
      throws AlluxioException, IOException {
    return retryRPC(
        new RpcCallableThrowsAlluxioTException<Long, FileSystemWorkerClientService.Client>() {
          @Override
          public Long call(FileSystemWorkerClientService.Client client)
              throws AlluxioTException, TException {
            return client.completeUfsFile(mSessionId, tempUfsFileId, options.toThrift());
          }
        });
  }

  @Override
  public long createUfsFile(final AlluxioURI path, final CreateUfsFileOptions options)
      throws AlluxioException, IOException {
    return retryRPC(
        new RpcCallableThrowsAlluxioTException<Long, FileSystemWorkerClientService.Client>() {
          @Override
          public Long call(FileSystemWorkerClientService.Client client)
              throws AlluxioTException, TException {
            return client.createUfsFile(mSessionId, path.toString(), options.toThrift());
          }
        });
  }

  @Override
  public InetSocketAddress getWorkerDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  @Override
  public long openUfsFile(final AlluxioURI path, final OpenUfsFileOptions options)
      throws AlluxioException, IOException {
    return retryRPC(
        new RpcCallableThrowsAlluxioTException<Long, FileSystemWorkerClientService.Client>() {
          @Override
          public Long call(FileSystemWorkerClientService.Client client)
              throws AlluxioTException, TException {
            return client.openUfsFile(mSessionId, path.toString(), options.toThrift());
          }
        });
  }

  @Override
  public void sessionHeartbeat(RetryPolicy retryPolicy) throws IOException, InterruptedException {
    TException exception;
    do {
      FileSystemWorkerClientService.Client client = mClientHeartbeatPool.acquire();
      try {
        client.sessionHeartbeat(mSessionId, null);
        Metrics.FILE_SYSTEM_WORKER_HEARTBEATS.inc();
        return;
      } catch (AlluxioTException | ThriftIOException e) {
        exception = e;
        LOG.warn(e.getMessage());
      } catch (TException e) {
        client.getOutputProtocol().getTransport().close();
        exception = e;
        LOG.warn(e.getMessage());
      } finally {
        mClientHeartbeatPool.release(client);
      }
    } while (retryPolicy.attemptRetry());
    Preconditions.checkNotNull(exception);
    throw new IOException(exception);
  }

  /**
   * Metrics related to the {@link RetryHandlingFileSystemWorkerClient}.
   */
  public static final class Metrics {
    private static final Counter FILE_SYSTEM_WORKER_HEARTBEATS =
        MetricsSystem.clientCounter("FileSystemWorkerHeartbeats");

    private Metrics() {} // prevent instantiation
  }
}
