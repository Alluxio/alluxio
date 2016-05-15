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

  private final WorkerNetAddress mWorkerNetAddress;
  private final ExecutorService mExecutorService;
  private final HeartbeatExecutor mHeartbeatExecutor;

  private final ClientMetrics mClientMetrics;
  private FileSystemWorkerClientService.Client mClient;
  private Future<?> mHeartbeat;
  // This is the address of the data server on the worker.
  private InetSocketAddress mWorkerDataServerAddress;
  private long mSessionId;

  public FileSystemWorkerClient(WorkerNetAddress workerNetAddress, ExecutorService executorService,
      Configuration conf, long sessionId, ClientMetrics metrics) {
    super(NetworkAddressUtils.getRpcPortSocketAddress(workerNetAddress), conf, "FileSystemWorker");
    mWorkerNetAddress = workerNetAddress;
    mWorkerDataServerAddress = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mSessionId = sessionId;
    mClientMetrics = Preconditions.checkNotNull(metrics);
    mHeartbeatExecutor = new FileSystemWorkerClientHeartbeatExecutor(this);
  }

  @Override
  protected void afterConnect() throws IOException {
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

  @Override
  protected synchronized void afterDisconnect() {
    if (mHeartbeat != null) {
      mHeartbeat.cancel(true);
    }
  }

  @Override
  protected synchronized void beforeDisconnect() {
    // Heartbeat to send the client metrics.
    if (mHeartbeatExecutor != null) {
      mHeartbeatExecutor.heartbeat();
    }
  }

  @Override
  protected AlluxioService.Client getClient() {
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

  public synchronized void cancelUfsFile(final long sessionId, final long tempUfsFileId,
      final CancelUfsFileOptions options) throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.cancelUfsFile(sessionId, tempUfsFileId, options.toThrift());
        return null;
      }
    });
  }

  public synchronized void closeUfsFile(final long sessionId, final long tempUfsFileId,
      final CloseUfsFileOptions options) throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.closeUfsFile(sessionId, tempUfsFileId, options.toThrift());
        return null;
      }
    });
  }

  public synchronized void completeUfsFile(final long sessionId, final long tempUfsFileId,
      final CompleteUfsFileOptions options) throws AlluxioException, IOException {
    retryRPC(new RpcCallableThrowsAlluxioTException<Void>() {
      @Override
      public Void call() throws AlluxioTException, TException {
        mClient.completeUfsFile(sessionId, tempUfsFileId, options.toThrift());
        return null;
      }
    });
  }

  public synchronized long createUfsFile(final long sessionId, final AlluxioURI path,
      final CreateUfsFileOptions options) throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.createUfsFile(sessionId, path.toString(), options.toThrift());
      }
    });
  }

  public InetSocketAddress getWorkerDataServerAddress() {
    return mWorkerDataServerAddress;
  }

  public synchronized long openUfsFile(final long sessionId, final AlluxioURI path,
      final OpenUfsFileOptions options) throws AlluxioException, IOException {
    return retryRPC(new RpcCallableThrowsAlluxioTException<Long>() {
      @Override
      public Long call() throws AlluxioTException, TException {
        return mClient.openUfsFile(sessionId, path.toString(), options.toThrift());
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
