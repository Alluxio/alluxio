package tachyon.master;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;

/**
 * Heartbeat executor for master client.
 */
class MasterClientHeartbeatExecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final MasterClient mClient;

  public MasterClientHeartbeatExecutor(MasterClient client) {
    mClient = client;
  }

  @Override
  public void heartbeat() {
    try {
      mClient.user_heartbeat();
    } catch (IOException e) {
      mClient.close();
      Throwables.propagate(e);
    }
  }
}
