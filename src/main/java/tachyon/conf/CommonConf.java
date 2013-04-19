package tachyon.conf;

/**
 * Configurations shared by master and workers.
 */
public class CommonConf extends Utils {
  private static CommonConf COMMON_CONF = null;

  public final String TACHYON_HOME;
  public final String UNDERFS_ADDRESS;
  public final String DATA_FOLDER;
  public final String WORKERS_FOLDER;

  private CommonConf() {
    TACHYON_HOME = getProperty("tachyon.home");
    UNDERFS_ADDRESS = getProperty("tachyon.underfs.address", TACHYON_HOME);
    DATA_FOLDER = UNDERFS_ADDRESS + getProperty("tachyon.data.folder", "/tachyon/data");
    WORKERS_FOLDER = UNDERFS_ADDRESS + getProperty("tachyon.workers.folder", "/tachyon/workers");
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
