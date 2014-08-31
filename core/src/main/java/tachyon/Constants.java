package tachyon;

/**
 * System wide constants
 */
public class Constants {
  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final int GB = MB * 1024;
  public static final long TB = GB * 1024L;
  public static final long PB = TB * 1024L;

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";

  public static final int SECOND_MS = 1000;
  public static final int MINUTE_MS = SECOND_MS * 60;
  public static final int HOUR_MS = MINUTE_MS * 60;
  public static final int DAY_MS = HOUR_MS * 24;

  public static final String SCHEME = "tachyon";
  public static final String HEADER = SCHEME + "://";

  public static final String SCHEME_FT = "tachyon-ft";
  public static final String HEADER_FT = SCHEME_FT + "://";

  public static final int DEFAULT_MASTER_PORT = 19998;
  public static final int DEFAULT_MASTER_WEB_PORT = DEFAULT_MASTER_PORT + 1;
  public static final int DEFAULT_WORKER_PORT = 29998;
  public static final int DEFAULT_WORKER_DATA_SERVER_PORT = DEFAULT_WORKER_PORT + 1;

  public static final int DEFAULT_BLOCK_SIZE_BYTE = 512 * MB;

  public static final String PATH_SEPARATOR = "/";

  public static final int WORKER_BLOCKS_QUEUE_SIZE = 10000;

  public static final String LOGGER_TYPE = System.getProperty("tachyon.logger.type", "");
  public static final boolean DEBUG = Boolean
      .valueOf(System.getProperty("tachyon.debug", "false"));

  /**
   * Version 1 [Before 0.5.0] Customized ser/de based.
   * Version 2 [0.5.0] Starts to use JSON.
   * Version 3 [0.6.0] Add lastModificationTimeMs to inode.
   */
  public static final int JOURNAL_VERSION = 2;
}
