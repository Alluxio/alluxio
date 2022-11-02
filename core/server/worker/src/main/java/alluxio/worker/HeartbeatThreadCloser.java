package alluxio.worker;

import alluxio.heartbeat.HeartbeatThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * The class is responsible for closing block worker threads.
 * Before submit a HeartBeatThread, it should be registered in a Closer.
 * So we can easily close worker threads in the future.
 */

public class HeartbeatThreadCloser implements Runnable, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatThreadCloser.class);

  private final HeartbeatThread mHeartBeatThread;

  /**
   * Initialise HeartbeatThreadCloser.
   * @param heartbeatThread
   */
  public HeartbeatThreadCloser(HeartbeatThread heartbeatThread) {
    mHeartBeatThread = heartbeatThread;
  }

  @Override
  public void run() {
    mHeartBeatThread.run();
    LOG.info("A HeartBeat thread has been closed.");
  }

  @Override
  public void close() throws IOException {
    mHeartBeatThread.closeHeartBeat();
  }
}
