package alluxio;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Utility class for fetching configuration across different types of Alluxio processes.
 */
public class Context {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * @return the configuration retrieved from the context of the current process
   */
  public static Configuration getConf() {
    try {
      Class clazz;
      switch (AlluxioProcess.getType()) {
        case MASTER:
          clazz = Class.forName("alluxio.master.MasterContext");
          break;
        case WORKER:
          clazz = Class.forName("alluxio.worker.WorkerContext");
          break;
        default:
          clazz = Class.forName("alluxio.client.ClientContext");
      }
      Method method = clazz.getMethod("getConf");
      return (Configuration) method.invoke(null);
    } catch (Exception e) {
      LOG.warn("failed to retrieve configuration from the context of the current process");
      Throwables.propagate(e);
    }
    return null;
  }

  private Context() {} // prevent instantiation
}
