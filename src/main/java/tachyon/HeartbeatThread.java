package tachyon;

import org.apache.log4j.Logger;

/**
 * Thread class to execute a heartbeat periodically.
 */
public class HeartbeatThread implements Runnable {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final String THREAD_NAME;
  private final HeartbeatExecutor EXECUTOR;
  private final long FIXED_EXECUTION_INTERVAL_MS;

  /**
   * @param threadName
   * @param hbExecutor
   * @param fixedExecutionIntervalMs Sleep time between different heartbeat.
   */
  public HeartbeatThread(String threadName, HeartbeatExecutor hbExecutor, 
      long fixedExecutionIntervalMs) {
    THREAD_NAME = threadName;
    EXECUTOR = hbExecutor;
    FIXED_EXECUTION_INTERVAL_MS = fixedExecutionIntervalMs;
  }

  public void run() {
    while (true) {
      long lastMs = System.currentTimeMillis();
      EXECUTOR.heartbeat();
      try {
        long executionTimeMs = System.currentTimeMillis() - lastMs;
        if (executionTimeMs > FIXED_EXECUTION_INTERVAL_MS) {
          LOG.error(THREAD_NAME + " last execution took " + executionTimeMs + " ms. Longer than " +
              " the FIXED_EXECUTION_INTERVAL_MS "+ FIXED_EXECUTION_INTERVAL_MS);
        } else {
          Thread.sleep(FIXED_EXECUTION_INTERVAL_MS - executionTimeMs);
        }
      } catch (InterruptedException e) {
        LOG.info(e.getMessage(), e);
      }
    }
  }
}
