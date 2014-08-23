package tachyon;

import org.apache.log4j.Logger;

/**
 * Thread class to execute a heartbeat periodically.
 * This Thread is daemonic, so it will not prevent the JVM from exiting.
 */
public class HeartbeatThread extends Thread {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final String mThreadName;
  private final HeartbeatExecutor mExecutor;
  private final long mFixedExecutionIntervalMs;

  private volatile boolean mIsShutdown = false;

  /**
   * @param threadName
   * @param hbExecutor
   * @param fixedExecutionIntervalMs
   *          Sleep time between different heartbeat.
   */
  public HeartbeatThread(String threadName, HeartbeatExecutor hbExecutor,
      long fixedExecutionIntervalMs) {
    mThreadName = threadName;
    mExecutor = hbExecutor;
    mFixedExecutionIntervalMs = fixedExecutionIntervalMs;
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (!mIsShutdown) {
        long lastMs = System.currentTimeMillis();
        mExecutor.heartbeat();
        long executionTimeMs = System.currentTimeMillis() - lastMs;
        if (executionTimeMs > mFixedExecutionIntervalMs) {
          LOG.warn(mThreadName + " last execution took " + executionTimeMs + " ms. Longer than "
              + " the mFixedExecutionIntervalMs " + mFixedExecutionIntervalMs);
        } else {
          Thread.sleep(mFixedExecutionIntervalMs - executionTimeMs);
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
