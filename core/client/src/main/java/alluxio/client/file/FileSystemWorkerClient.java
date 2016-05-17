/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AbstractClient;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;

import alluxio.client.file.options.CancelUfsFileOptions;
import alluxio.client.file.options.CloseUfsFileOptions;
import alluxio.client.file.options.CompleteUfsFileOptions;
import alluxio.client.file.options.CreateUfsFileOptions;
import alluxio.client.file.options.OpenUfsFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ClientMetrics;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Client for talking to a file system worker server. It keeps sending keep alive messages to the
 * worker server to preserve its state.
 *
 * Since {@link alluxio.thrift.FileSystemWorkerClientService} is not thread safe, this class
 * guarantees thread safety.
 */
// TODO(calvin): Session logic can be abstracted
@ThreadSafe
public class FileSystemWorkerClient extends AbstractClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Executor service for running the heartbeat thread. */
  private final ExecutorService mExecutorService;
  /** Heartbeat executor for the session heartbeat thread. */
  private final HeartbeatExecutor mHeartbeatExecutor;
  /** Metrics object. */
  private final ClientMetrics mClientMetrics;
  /** Address of the data server on the worker. */
  private final InetSocketAddress mWorkerDataServerAddress;

  /** Underlying thrift RPC client which executes the operations. */
  private FileSystemWorkerClientService.Client mClient;
  /** The current heartbeat thread, this will be updated each time this client connects. */
  private Future<?> mHeartbeat;
  /** The current session id, managed by the caller. */
  private long mSessionId;

  /**
   * Constructor for a client that communicates with the {@link FileSystemWorkerClientService}.
   *
   * @param workerNetAddress the worker address to connect to
   * @param executorService the executor service to run this client's heartbeat thread
   * @param conf the configuration to use
   * @param sessionId the session id to use, this should be unique
   * @param metrics the metrics object to send any metrics through
   */
  public FileSystemWorkerClient(WorkerNetAddress workerNetAddress, ExecutorService executorService,
      Configuration conf, long sessionId, ClientMetrics metrics) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress), conf, "FileSystemWorker");
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mSessionId = sessionId;
    mClientMetrics = Preconditions.checkNotNull(metrics);
    mHeartbeatExecutor = new FileSystemWorkerClientHeartbeatExecutor(this);
  }

  /**
   * Creates the underlying thrift client and starts the heartbeat thread, unless one is already
   * running.
   *
   * @throws IOException if the thrift client cannot be created
   */
  @Override
  protected synchronized void afterConnect() throws IOException {
    mClient = new FileSystemWorkerClientService.Client(mProtocol);
    // only start the heartbeat thread if the connection is successful and if there is not
    // another heartbeat thread running
    if (mHeartbeat == null || mHeartbeat.isCancelled() || mHeartbeat.isDone()) {
      final int interval = mConfiguration.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS);
      mHeartbeat =
          mExecutorService.submit(new HeartbeatThread(HeartbeatContext.WORKER_CLIENT,
              mHeartbeatExecutor, interval));
    }
  }

  /**
   * Cancels the current heartbeat thread.
   */
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
    return Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION;
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
  public synchronized void cancelUfsFile(final long tempUfsFileId,
      final CancelUfsFileOptions options) throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.cancelUfsFile(mSessionId, tempUfsFileId, options.toThrift());
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
  public synchronized void closeUfsFile(final long tempUfsFileId, final CloseUfsFileOptions options)
      throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.closeUfsFile(mSessionId, tempUfsFileId, options.toThrift());
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
  public synchronized long completeUfsFile(final long tempUfsFileId,
      final CompleteUfsFileOptions options) throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.completeUfsFile(mSessionId, tempUfsFileId, options.toThrift());
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
  public synchronized long createUfsFile(final AlluxioURI path, final CreateUfsFileOptions options)
      throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.createUfsFile(mSessionId, path.toString(), options.toThrift());
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
  public synchronized long openUfsFile(final AlluxioURI path, final OpenUfsFileOptions options)
      throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.openUfsFile(mSessionId, path.toString(), options.toThrift());
      }
    });
  }

  /**
   * Called only by {@link FileSystemWorkerClientHeartbeatExecutor}, encapsulates
   * {@link #sessionHeartbeat()} in order to cancel and cleanup the heartbeating thread in case of
   * failures.
   */
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

  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * temporary files and updates the worker's metrics.
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
}
