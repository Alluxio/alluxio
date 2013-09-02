package tachyon;

/**
 * System wide constants
 */
public class Constants {
  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final int GB = MB * 1024;
  public static final long TB = GB * 1024L;

  public static final long SECOND_MS = 1000;
  public static final long MINUTE_MS = SECOND_MS * 60;
  public static final long HOUR_MS = MINUTE_MS * 60;
  public static final long DAY_MS = HOUR_MS * 24;
  
  public static final String FT_HEADER = "tachyon_ft://";

  public static final int DEFAULT_MASTER_PORT = 19998;
  public static final int DEFAULT_MASTER_WEB_PORT = DEFAULT_MASTER_PORT + 1;
  public static final int DEFAULT_WORKER_PORT = 29998;
  public static final int DEFAULT_WORKER_DATA_SERVER_PORT = DEFAULT_WORKER_PORT + 1;

  public static final int DEFAULT_BLOCK_SIZE_BYTE = 512 * MB;

  public static final String PATH_SEPARATOR = "/";
  public static final int MAX_COLUMNS = 1000;
  public static final int MAX_TABLE_METADATA_BYTE = MB * 5;
  public static final int WORKER_BLOCKS_QUEUE_SIZE = 10000;

  public static final String LOGGER_TYPE = System.getProperty("tachyon.logger.type", "");
  public static final boolean DEBUG = Boolean.valueOf(System.getProperty("tachyon.debug", "false"));
  
  public static final int JOURNAL_VERSION = 1; // The version keeps increasing.
}
