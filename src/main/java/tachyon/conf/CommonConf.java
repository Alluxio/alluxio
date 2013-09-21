package tachyon.conf;

/**
 * Configurations shared by master and workers.
 */
public class CommonConf extends Utils {
  private static CommonConf COMMON_CONF = null;

  public final String TACHYON_HOME;
  public final String UNDERFS_ADDRESS;
  public final String UNDERFS_DATA_FOLDER;
  public final String UNDERFS_WORKERS_FOLDER;
  public final boolean USE_FIXED_CHECKPOINT;

  public final boolean USE_ZOOKEEPER;
  public final String ZOOKEEPER_ADDRESS;
  public final String ZOOKEEPER_ELECTION_PATH;
  public final String ZOOKEEPER_LEADER_PATH;

  private CommonConf() {
    TACHYON_HOME = getProperty("tachyon.home");
    UNDERFS_ADDRESS = getProperty("tachyon.underfs.address", TACHYON_HOME);
    UNDERFS_DATA_FOLDER = UNDERFS_ADDRESS + getProperty("tachyon.data.folder", "/tachyon/data");
    UNDERFS_WORKERS_FOLDER = 
        UNDERFS_ADDRESS + getProperty("tachyon.workers.folder", "/tachyon/workers");
    FIXED_CHECKPOINT = getBooleanProperty("tachyon.fixed.checkpoint", false); 

    USE_ZOOKEEPER = getBooleanProperty("tachyon.usezookeeper", false);
    if (USE_ZOOKEEPER) {
      ZOOKEEPER_ADDRESS = getProperty("tachyon.zookeeper.address");
      ZOOKEEPER_ELECTION_PATH = getProperty("tachyon.zookeeper.election.path", "/election");
      ZOOKEEPER_LEADER_PATH = getProperty("tachyon.zookeeper.leader.path", "/leader");
    } else {
      ZOOKEEPER_ADDRESS = null;
      ZOOKEEPER_ELECTION_PATH = null;
      ZOOKEEPER_LEADER_PATH = null;
    }
  }

  public static synchronized CommonConf get() {
    if (COMMON_CONF == null) {
      COMMON_CONF = new CommonConf();
    }

    return COMMON_CONF;
  }

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    COMMON_CONF = null;
  }
}
