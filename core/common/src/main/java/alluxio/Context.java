package alluxio;

import com.google.common.base.Throwables;

import java.lang.reflect.Method;

/**
 * Utility class for fetching configuration across different types of Alluxio processes.
 */
public class Context {
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
      Throwables.propagate(e);
    }
    return null;
  }

  private Context() {} // prevent instantiation
}
