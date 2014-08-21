package tachyon.master;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;

/**
 * Heartbeat executor for master client.
 */
class MasterClientHeartbeatExecutor implements HeartbeatExecutor {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final MasterClient CLIENT;
  private final long MAX_NONE_ACCESS_INTERVAL;

  public MasterClientHeartbeatExecutor(MasterClient client, long maxNoneAccessIntervalMs) {
    CLIENT = client;
    MAX_NONE_ACCESS_INTERVAL = maxNoneAccessIntervalMs;
  }

  @Override
  public void heartbeat() {
    long internalMs = System.currentTimeMillis() - CLIENT.getLastAccessedMs();
    if (internalMs > MAX_NONE_ACCESS_INTERVAL) {
      LOG.debug("The last Heartbeat was " + internalMs + " ago.");
      CLIENT.close();
    }
  }
}
