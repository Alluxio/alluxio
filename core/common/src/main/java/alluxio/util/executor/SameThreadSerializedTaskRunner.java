package alluxio.util.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of the SerializedTaskRunner which simply executes the tasks as they are added
 * within the same thread. The tasks are never queued.
 */
public class SameThreadSerializedTaskRunner extends SerializedTaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SameThreadSerializedTaskRunner.class);

  /**
   * Create a new instance of {@link SameThreadSerializedTaskRunner}.
   */
  public SameThreadSerializedTaskRunner() {
    super();
  }

  @Override
  public boolean addTask(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException e) {
      LOG.warn("Runtime exception thrown when executing task: ", e);
    }
    return true;
  }
}
