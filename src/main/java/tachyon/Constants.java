package tachyon;

/**
 * System wide constants
 */
public class Constants {
  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final long GB = MB * 1024L;
  public static final long TB = GB * 1024L;
  public static final long TWO_32 = 1L << 32;

  public static final int DEFAULT_MASTER_PORT = 9999;
  public static final int DEFAULT_MASTER_WEB_PORT = 9998;
  public static final int DEFAULT_WORKER_PORT = 10000;
  public static final int DEFAULT_WORKER_DATA_SERVER_PORT = 10001;

  public static final String PATH_SEPARATOR = "/";
  public static final int MAX_COLUMNS = 100;
  public static final int WORKER_FILES_QUEUE_SIZE = 10000;
}
