package tachyon.perf.conf;

import java.io.File;

import org.apache.log4j.Logger;

/**
 * Tachyon-Perf Configurations
 */
public class PerfConf extends Utils {
  private static final Logger LOG = Logger.getLogger("");

  private static PerfConf sPerfConf = null;

  public static synchronized PerfConf get() {
    if (sPerfConf == null) {
      sPerfConf = new PerfConf();
    }
    return sPerfConf;
  }

  public final String TACHYON_PERF_HOME;

  public final String OUT_FOLDER;
  public final boolean STATUS_DEBUG;
  public final String WORK_DIR;
  public final int THREADS_NUM;

  public final boolean FAILED_THEN_ABORT;
  public final int FAILED_PERCENTAGE;

  public final String TACHYON_PERF_MASTER_HOSTNAME;
  public final int TACHYON_PERF_MASTER_PORT;
  public final long UNREGISTER_TIMEOUT_MS;

  private PerfConf() {
    if (System.getProperty("tachyon.perf.home") == null) {
      LOG.warn("tachyon.perf.home is not set. Using /tmp/tachyon_perf_default_home as default.");
      File file = new File("/tmp/tachyon_perf_default_home");
      if (!file.exists()) {
        file.mkdirs();
      }
    }
    TACHYON_PERF_HOME = getProperty("tachyon.perf.home", "/tmp/tachyon_perf_default_home");
    STATUS_DEBUG = getBooleanProperty("tachyon.perf.status.debug", false);
    WORK_DIR = getProperty("tachyon.perf.work.dir", "/tmp/tachyon-perf-workspace");
    OUT_FOLDER = getProperty("tachyon.perf.out.dir", TACHYON_PERF_HOME + "/result");
    THREADS_NUM = getIntProperty("tachyon.perf.threads.num", 1);

    FAILED_THEN_ABORT = getBooleanProperty("tachyon.perf.failed.abort", true);
    FAILED_PERCENTAGE = getIntProperty("tachyon.perf.failed.percentage", 1);

    TACHYON_PERF_MASTER_HOSTNAME = getProperty("tachyon.perf.master.hostname", "master");
    TACHYON_PERF_MASTER_PORT = getIntProperty("tachyon.perf.master.port", 23333);
    UNREGISTER_TIMEOUT_MS = getLongProperty("tachyon.perf.unregister.timeout.ms", 10000);
  }
}
