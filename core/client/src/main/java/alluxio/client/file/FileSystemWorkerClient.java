package alluxio.client.file;

import alluxio.AbstractClient;
import alluxio.Configuration;
import alluxio.Constants;

import alluxio.exception.ConnectionFailedException;
import alluxio.heartbeat.HeartbeatExecutor;
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
// TODO(calvin): Session tracking logic can be abstracted
@ThreadSafe
public class FileSystemWorkerClient extends AbstractClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int CONNECTION_RETRY_TIMES = 5;

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
