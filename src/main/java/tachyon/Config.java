package tachyon;

import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All constants in the system. This should only be visible to the System. Should not be visible to 
 * user code.
 * 
 * @author haoyuan
 */
public class Config {
  public static final boolean IS_SYSTEM;
  public static final String TACHYON_HOME;

  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;
  public static final long TWO_32 = 1L << 32;

  public static final String MASTER_LOG_FILE;
  public static final String MASTER_CHECKPOINT_FILE;
  public static final int MASTER_HEARTBEAT_INTERVAL_MS = 1000;
  public static final int MASTER_SELECTOR_THREADS = 3;
  public static final int MASTER_QUEUE_SIZE_PER_SELECTOR = 3000;
  public static final int MASTER_WORKER_THREADS = 128;
  public static final String MASTER_HOSTNAME;
  public static final int MASTER_PORT = 9999;
  public static final int MASTER_WEB_PORT = 9998;
  public static final boolean MASTER_SUBSUME_HDFS;

  public static final String WORKER_DATA_FOLDER;
  public static final long WORKER_MEMORY_SIZE;
  public static final long WORKER_TIMEOUT_MS = 3 * 1000;
  public static final long WORKER_HEARTBEAT_TIMEOUT_MS = 10 * 1000;;
  public static final int WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS = 1000;
  public static final int WORKER_DATA_ACCESS_QUEUE_SIZE = 10000;
  public static final int WORKER_SELECTOR_THREADS = 2;
  public static final int WORKER_QUEUE_SIZE_PER_SELECTOR = 3000;
  public static final int WORKER_WORKER_THREADS = 128;
  public static final int WORKER_PORT = 10000;
  public static final int WORKER_DATA_SERVER_PORT = 10001;
  public static final String WORKER_HDFS_FOLDER = "/tachyon/workers";

  public static final int USER_FAILED_SPACE_REQUEST_LIMITS = 3;
  public static final String USER_TEMP_RELATIVE_FOLDER = "users";
  public static final long USER_TIMEOUT_MS = 3 * 1000;;
  public static final long USER_QUOTA_UNIT_BYTES = 25 * MB;
  public static final int USER_BUFFER_PER_PARTITION_BYTES = 1 * MB;
  public static final int USER_HEARTBEAT_INTERVAL_MS = 1000;

  public static final int CONECTION_MAX_TRY = 3;
  public static final String HDFS_TEMP_FILE = "_temporary";
  public static final String HDFS_ADDRESS;
  public static final String HDFS_DATA_FOLDER = "/tachyon/data";
  public static final boolean USING_HDFS;

  public static final ArrayList<String> WHITELIST = new ArrayList<String>();
  public static final ArrayList<String> PINLIST = new ArrayList<String>();

  public static final boolean DEBUG;

  private static final Logger LOG = LoggerFactory.getLogger(Config.class);

  private static String getNonNullProperty(String property, String defaultValue) {
    String ret = System.getProperty(property);
    if (ret == null) {
      if (IS_SYSTEM) {
        CommonUtils.illegalArgumentException(property + " is not configured.");
      }
      ret = defaultValue;
    } else {
      LOG.info(property + " : " + ret);
    }
    return ret;
  }

  private static String getProperty(String property, String defaultValue) {
    String ret = System.getProperty(property);
    String msg = "";
    if (ret == null) {
      ret = defaultValue;
      msg = " users default value";
    }
    LOG.info(property + msg + " : " + ret);
    return ret;
  }

  static {
    IS_SYSTEM = Boolean.parseBoolean(getProperty("tachyon.is.system", "false"));
    TACHYON_HOME = getNonNullProperty("tachyon.home", null);

    if (TACHYON_HOME != null) {
      MASTER_LOG_FILE = getProperty("tachyon.master.log.file", TACHYON_HOME + "/logs/tachyon_log.data");
      MASTER_CHECKPOINT_FILE = getProperty("tachyon.master.checkpoint.file", TACHYON_HOME + "/logs/tachyon_checkpoint.data");
    } else {
      MASTER_LOG_FILE = null;
      MASTER_CHECKPOINT_FILE = null;
    }
    MASTER_HOSTNAME = getProperty("tachyon.master.hostname", "localhost");
    MASTER_SUBSUME_HDFS = Boolean.parseBoolean(getProperty("tachyon.master.subsume.hdfs", "false"));

    WORKER_DATA_FOLDER = getProperty("tachyon.worker.data.folder", "/mnt/ramfs");
    WORKER_MEMORY_SIZE = CommonUtils.parseMemorySize(
        getProperty("tachyon.worker.memory.size", "2GB"));

    HDFS_ADDRESS = getProperty("tachyon.hdfs.address", null);
    if (HDFS_ADDRESS == null) {
      LOG.warn("tachyon.hdfs.address was not set.");
    }
    USING_HDFS = ((HDFS_ADDRESS != null) && (HDFS_ADDRESS.startsWith("hdfs")));

    WHITELIST.addAll(Arrays.asList(getProperty("tachyon.whitelist", "/").split(";")));
    String tPinList = getProperty("tachyon.pinlist", null);
    if (tPinList != null) {
      PINLIST.addAll(Arrays.asList(tPinList.split(";")));
    }

    DEBUG = Boolean.parseBoolean(getProperty("tachyon.debug", "false"));
  }
}