package tachyon.conf;

import tachyon.Constants;

public class UserConf extends Utils {

  private static UserConf USER_CONF = null;

  public final int FAILED_SPACE_REQUEST_LIMITS;
  public final long QUOTA_UNIT_BYTES;
  public final int FILE_BUFFER_BYTES;
  public final long HEARTBEAT_INTERVAL_MS;
  public final long MASTER_CLIENT_TIMEOUT_MS;

  public final long DEFAULT_BLOCK_SIZE_BYTE = 512 * Constants.MB;

  private UserConf() {
    FAILED_SPACE_REQUEST_LIMITS = getIntProperty("tachyon.user.failed.space.request.limits", 3);
    QUOTA_UNIT_BYTES = getLongProperty("tachyon.user.quota.unit.bytes", 8 * Constants.MB);
    FILE_BUFFER_BYTES = getIntProperty("tachyon.user.file.buffer.bytes", Constants.MB);
    HEARTBEAT_INTERVAL_MS = getLongProperty("tachyon.user.heartbeat.interval.ms", 1000);
    MASTER_CLIENT_TIMEOUT_MS = getLongProperty("tachyon.user.master.client.timeout.ms", 10000);
  }

  public static synchronized UserConf get() {
    if (USER_CONF == null) {
      USER_CONF = new UserConf();
    }

    return USER_CONF;
  }

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    USER_CONF = null;
  }
}
