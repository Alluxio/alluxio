package tachyon.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;

/**
 * Heartbeat executor for master client.
 */
class MasterClientHeartbeatExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final MasterClient mClient;
  private final long mMaxNoneAccessIntervalMs;

  public MasterClientHeartbeatExecutor(MasterClient client, long maxNoneAccessIntervalMs) {
    mClient = client;
    mMaxNoneAccessIntervalMs = maxNoneAccessIntervalMs;
  }

  @Override
  public void heartbeat() {
    long internalMs = System.currentTimeMillis() - mClient.getLastAccessedMs();
    if (internalMs > mMaxNoneAccessIntervalMs) {
      LOG.debug("The last Heartbeat was {} ago.", internalMs);
      mClient.close();
    }
  }
}
