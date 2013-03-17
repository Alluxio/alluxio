package tachyon;

import org.apache.log4j.Logger;

public class HeartbeatThread implements Runnable {
  private final Logger LOG = Logger.getLogger(Config.LOGGER_TYPE);
  private final HeartbeatExecutor HEARTBEAT;
  private final int SLEEP_INTERVAL_MS;

  public HeartbeatThread(HeartbeatExecutor hbExecutor, int sleepIntervalMs) {
    HEARTBEAT = hbExecutor;
    SLEEP_INTERVAL_MS = sleepIntervalMs;
  }

  public void run() {
    while (true) {
      HEARTBEAT.heartbeat();
      try {
        Thread.sleep(SLEEP_INTERVAL_MS);
      } catch (InterruptedException e) {
        LOG.info(e.getMessage(), e);
      }
    }
  }
}
