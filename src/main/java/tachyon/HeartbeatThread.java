package tachyon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatThread implements Runnable {
  private final Logger LOG = LoggerFactory.getLogger(HeartbeatThread.class);
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
