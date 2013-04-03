package tachyon.conf;

import tachyon.CommonUtils;

public class WorkerConf extends Utils {
  private static WorkerConf WORKER_CONF = null;

  public final String DATA_FOLDER;
  public final long MEMORY_SIZE;
  public final long HEARTBEAT_TIMEOUT_MS;
  public final int TO_MASTER_HEARTBEAT_INTERVAL_MS;
  public final int SELECTOR_THREADS;
  public final int QUEUE_SIZE_PER_SELECTOR;
  public final int SERVER_THREADS;
  public final int USER_TIMEOUT_MS = 60 * 1000;
  public final String USER_TEMP_RELATIVE_FOLDER = "users";

  private WorkerConf() {
    DATA_FOLDER = getProperty("tachyon.worker.data.folder", "/mnt/ramdisk");
    MEMORY_SIZE = CommonUtils.parseMemorySize(getProperty("tachyon.worker.memory.size"));
    HEARTBEAT_TIMEOUT_MS = getIntProperty("tachyon.worker.heartbeat.timeout.ms", 10 * 1000);
    TO_MASTER_HEARTBEAT_INTERVAL_MS = 
        getIntProperty("tachyon.worker.to.master.heartbeat.interval.ms", 1000);
    SELECTOR_THREADS = getIntProperty("tachyon.worker.selector.threads", 3);
    QUEUE_SIZE_PER_SELECTOR = getIntProperty("tachyon.worker.queue.size.per.selector", 3000);
    SERVER_THREADS = getIntProperty("tachyon.worker.server.threads", 128);
  }

  public static synchronized WorkerConf get() {
    if (WORKER_CONF == null) {
      WORKER_CONF = new WorkerConf();
    }

    return WORKER_CONF;
  }
}
