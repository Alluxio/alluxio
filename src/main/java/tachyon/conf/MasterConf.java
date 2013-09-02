package tachyon.conf;

import java.util.ArrayList;
import java.util.Arrays;

import tachyon.Constants;

/**
 * Configurations used by master only.
 */
public class MasterConf extends Utils {
  private static MasterConf MASTER_CONF = null;

  public final String JOURNAL_FOLDER;
  public final String HOSTNAME;
  public final int PORT;
  public final int WEB_PORT;
  public final String TEMPORARY_FOLDER;

  public final int HEARTBEAT_INTERVAL_MS;
  public final int SELECTOR_THREADS;
  public final int QUEUE_SIZE_PER_SELECTOR;
  public final int SERVER_THREADS;
  public final int WORKER_TIMEOUT_MS;

  public final ArrayList<String> WHITELIST = new ArrayList<String>();
  public final ArrayList<String> PINLIST = new ArrayList<String>();

  private MasterConf() {
    JOURNAL_FOLDER = getProperty("tachyon.master.journal.folder", 
        CommonConf.get().TACHYON_HOME + "/journal/");

    HOSTNAME = getProperty("tachyon.master.hostname", "localhost");
    PORT = getIntProperty("tachyon.master.port", Constants.DEFAULT_MASTER_PORT);
    WEB_PORT = getIntProperty("tachyon.master.web.port", Constants.DEFAULT_MASTER_WEB_PORT);
    TEMPORARY_FOLDER = getProperty("tachyon.master.hostname", "/tmp");

    HEARTBEAT_INTERVAL_MS = getIntProperty("tachyon.master.heartbeat.interval.ms", 1000);
    SELECTOR_THREADS = getIntProperty("tachyon.master.selector.threads", 3);
    QUEUE_SIZE_PER_SELECTOR = getIntProperty("tachyon.master.queue.size.per.selector", 3000);
    SERVER_THREADS = getIntProperty("tachyon.master.server.threads", 128);
    WORKER_TIMEOUT_MS = getIntProperty("tachyon.master.worker.timeout.ms", 10 * 1000);

    WHITELIST.addAll(Arrays.asList(getProperty("tachyon.master.whitelist", "/").split(";")));
    String tPinList = getProperty("tachyon.master.pinlist", null);
    if (tPinList != null && !tPinList.isEmpty()) {
      PINLIST.addAll(Arrays.asList(tPinList.split(";")));
    }
  }

  public static synchronized MasterConf get() {
    if (MASTER_CONF == null) {
      MASTER_CONF = new MasterConf();
    }

    return MASTER_CONF;
  }

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    MASTER_CONF = null;
  }
}
