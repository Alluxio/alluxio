package alluxio.proxy;

import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.MasterClientContext;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The Proxy will maintain a stateless heartbeat with the primary master.
 * This enables the admin to list all living Proxy instances in the cluster.
 */
@NotThreadSafe
public final class ProxyMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyMasterSync.class);

  /** The address of this proxy. */
  private final Address mAddress;

  /** Client for communication with the primary master. */
  private final RetryHandlingMetaMasterProxyClient mMasterClient;

  /**
   * Creates a new instance of {@link ProxyMasterSync}.
   *
   * @param address the proxy address
   * @param context the communication context
   * @param startTimeMs start time of this instance
   */
  public ProxyMasterSync(Address address, MasterClientContext context, long startTimeMs) {
    mAddress = address;
    mMasterClient = new RetryHandlingMetaMasterProxyClient(mAddress, context, startTimeMs);
    LOG.info("Proxy start time is {}", Instant.ofEpochMilli(startTimeMs));
  }

  /**
   * Heartbeats to the primary master node.
   */
  @Override
  public void heartbeat() {
    try {
      LOG.info("Heart beating to primary master");
      mMasterClient.proxyHeartbeat();
    } catch (IOException e) {
      // Log the error but do not shut down the proxy
      LOG.error("Failed to heartbeat to primary master", e);
      mMasterClient.disconnect();
    }
  }

  @Override
  public void close() {}
}
