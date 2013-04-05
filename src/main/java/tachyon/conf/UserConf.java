package tachyon.conf;

import tachyon.Constants;

public class UserConf extends Utils {
  private static UserConf USER_CONF = null;

  public final int FAILED_SPACE_REQUEST_LIMITS;
  public final long QUOTA_UNIT_BYTES;
  public final int FILE_BUFFER_BYTES;
  public final int HEARTBEAT_INTERVAL_MS;

  private UserConf() {
    FAILED_SPACE_REQUEST_LIMITS = getIntProperty("tachyon.user.failed.space.request.limits", 3);
    QUOTA_UNIT_BYTES = getLongProperty("tachyon.user.quota.unit.bytes", 16 * Constants.MB);
    FILE_BUFFER_BYTES = getIntProperty("tachyon.user.file.buffer.bytes", Constants.MB);
    HEARTBEAT_INTERVAL_MS = getIntProperty("tachyon.user.heartbeat.interval.ms", 1000);
  }

  public static synchronized UserConf get() {
    if (USER_CONF == null) {
      USER_CONF = new UserConf();
    }

    return USER_CONF;
  }
}
