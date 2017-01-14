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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.options.CancelUfsFileOptions;
import alluxio.client.file.options.CloseUfsFileOptions;
import alluxio.client.file.options.CompleteUfsFileOptions;
import alluxio.client.file.options.CreateUfsFileOptions;
import alluxio.client.file.options.OpenUfsFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Throwables;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
public class FileSystemWorkerClient
    extends AbstractThriftClient<FileSystemWorkerClientService.Client> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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

  private final ScheduledFuture<?> mHeartbeat;

  /**
   * Constructor for a client that communicates with the {@link FileSystemWorkerClientService}.
   *
   * @param clientPool the client pool
   * @param clientHeartbeatPool the client pool for heartbeat
   * @param workerNetAddress the worker address to connect to
   * @param sessionId the session id to use, this should be unique
   * @throws IOException if it fails to register the session with the worker specified
   */
  public FileSystemWorkerClient(
      FileSystemWorkerThriftClientPool clientPool,
      FileSystemWorkerThriftClientPool clientHeartbeatPool,
      WorkerNetAddress workerNetAddress, final long sessionId)
      throws IOException {
    mClientPool = clientPool;
    mClientHeartbeatPool = clientHeartbeatPool;

    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mSessionId = sessionId;

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
              // do nothing
            } catch (Exception e) {
              LOG.error("Failed to heartbeat for session " + sessionId, e);
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
      throw Throwables.propagate(e);
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

  /**
   * Cancels the file currently being written with the specified id. This file must have also
   * been created through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to cancel
   * @param options method options
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
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

  /**
   * Closes the file currently being written with the specified id. This file must have also
   * been opened through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to close
   * @param options method options
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
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

  /**
   * Completes the file currently being written with the specified id. This file must have also
   * been created through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to complete
   * @param options method options
   * @return the file size of the completed file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
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

  /**
   * Creates a new file in the UFS with the given path.
   *
   * @param path the path in the UFS to create, must not already exist
   * @param options method options
   * @return the worker specific file id to reference the created file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
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

  /**
   * @return the data server address of the worker this client is connected to
   */
  public InetSocketAddress getWorkerDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  /**
   * Opens an existing file in the UFS with the given path.
   *
   * @param path the path in the UFS to open, must exist
   * @param options method options
   * @return the worker specific file id to reference the opened file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
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

  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * temporary files.
   *
   * @throws IOException if an I/O error occurs
   * @throws InterruptedException if the heartbeat is interrupted
   */
  public void sessionHeartbeat() throws IOException, InterruptedException {
    FileSystemWorkerClientService.Client client = mClientHeartbeatPool.acquire();
    try {
      client.sessionHeartbeat(mSessionId, null);
    } catch (AlluxioTException e) {
      throw Throwables.propagate(e);
    } catch (ThriftIOException e) {
      throw new IOException(e);
    } catch (TException e) {
      client.getOutputProtocol().getTransport().close();
      throw new IOException(e);
    } finally {
      mClientHeartbeatPool.release(client);
    }
  }
}
