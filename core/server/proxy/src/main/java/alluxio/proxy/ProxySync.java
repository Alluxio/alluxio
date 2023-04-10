package alluxio.proxy;

import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.conf.Configuration;
import alluxio.grpc.Scope;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.MasterClientContext;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;
/**
 * If a master is detected as a standby master. It will set up its MetaMasterSync and manage its own
 * {@link RetryHandlingMetaMasterMasterClient} which helps communicate with the leader master.
 *
 * When running, the standby master will send its heartbeat to the leader master. The leader master
 * may respond to the heartbeat with a command which will be executed. After which, the task will
 * wait for the elapsed time since its last heartbeat has reached the heartbeat interval. Then the
 * cycle will continue.
 */
@NotThreadSafe
public final class ProxySync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ProxySync.class);
  private static final long UNINITIALIZED_MASTER_ID = -1L;

  /** The address of this standby master. */
  private final Address mMasterAddress;

  /** Client for communication with the leader master. */
  private final RetryHandlingMetaMasterProxyClient mMasterClient;

  private final long mStartTimeMs;

  /**
   * Creates a new instance of {@link MetaMasterSync}.
   *
   * @param masterAddress the master address
   * @param masterClient the meta master client
   */
  public ProxySync(Address masterAddress, MasterClientContext context, long startTimeMs) {
    mMasterAddress = masterAddress;
    mStartTimeMs = startTimeMs;
    mMasterClient = new RetryHandlingMetaMasterProxyClient(mMasterAddress, context, mStartTimeMs);
  }

  /**
   * Heartbeats to the leader master node.
   */
  @Override
  public void heartbeat() {
    LOG.info("Heart beating to primary");
    try {
      LOG.info("Heart beating to primary master");
      mMasterClient.proxyHeartbeat();
    } catch (IOException e) {
      // An error occurred, log and ignore it or error if heartbeat timeout is reached
      LOG.error("Failed to heartbeat to primary master", e);
      mMasterClient.disconnect();
    }
  }

  @Override
  public void close() {}
}