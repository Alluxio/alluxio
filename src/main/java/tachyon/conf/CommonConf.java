package tachyon.conf;

public class CommonConf extends Utils {
  private static CommonConf COMMON_CONF = null;

  public static final String LOGGER_TYPE = System.getProperty("tachyon.logger.type", "");

  public final boolean DEBUG;

  public final String HDFS_ADDRESS;
  public final String DATA_FOLDER;
  public final String WORKERS_FOLDER;
  public final boolean USING_HDFS;

  private CommonConf() {
    DEBUG = getBooleanProperty("tachyon.debug", false);
    HDFS_ADDRESS = getProperty("tachyon.hdfs.address", null);
    DATA_FOLDER = getProperty("tachyon.data.folder", "/tachyon/data");
    WORKERS_FOLDER = getProperty("tachyon.workers.folder", "/tachyon/workers");
    USING_HDFS = ((HDFS_ADDRESS != null) && (HDFS_ADDRESS.startsWith("hdfs")));
  }

  public static synchronized CommonConf get() {
    if (COMMON_CONF == null) {
      COMMON_CONF = new CommonConf();
    }

    return COMMON_CONF;
  }
}
