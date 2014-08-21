package tachyon;

import org.apache.log4j.Logger;

/**
 * Thread class to execute a heartbeat periodically.
 * This Thread is daemonic, so it will not prevent the JVM from exiting.
 */
public class HeartbeatThread extends Thread {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final String THREAD_NAME;
  private final HeartbeatExecutor EXECUTOR;
  private final long FIXED_EXECUTION_INTERVAL_MS;

  private volatile boolean mIsShutdown = false;

  /**
   * @param threadName
   * @param hbExecutor
   * @param fixedExecutionIntervalMs
   *          Sleep time between different heartbeat.
   */
  public HeartbeatThread(String threadName, HeartbeatExecutor hbExecutor,
      long fixedExecutionIntervalMs) {
    THREAD_NAME = threadName;
    EXECUTOR = hbExecutor;
    FIXED_EXECUTION_INTERVAL_MS = fixedExecutionIntervalMs;
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (!mIsShutdown) {
        long lastMs = System.currentTimeMillis();
        EXECUTOR.heartbeat();
        long executionTimeMs = System.currentTimeMillis() - lastMs;
        if (executionTimeMs > FIXED_EXECUTION_INTERVAL_MS) {
          LOG.warn(THREAD_NAME + " last execution took " + executionTimeMs + " ms. Longer than "
              + " the FIXED_EXECUTION_INTERVAL_MS " + FIXED_EXECUTION_INTERVAL_MS);
        } else {
          Thread.sleep(FIXED_EXECUTION_INTERVAL_MS - executionTimeMs);
        }
      }
    } catch (InterruptedException e) {
      if (!mIsShutdown) {
        LOG.error("Heartbeat Thread was interrupted ungracefully, shutting down...", e);
      }
    } catch (Exception e) {
      LOG.error("Uncaught exception in heartbeat executor, Heartbeat Thread shutting down", e);
    }
  }

  public void shutdown() {
    mIsShutdown = true;
    this.interrupt();
  }
}
