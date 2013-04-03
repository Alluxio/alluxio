package tachyon.conf;

import tachyon.Constants;

public class UserConf {
  private static UserConf USER_CONF = null;

  public final int FAILED_SPACE_REQUEST_LIMITS = 3;
  public final long QUOTA_UNIT_BYTES = 16 * Constants.MB;
  public final int BUFFER_PER_PARTITION_BYTES = 1 * Constants.MB;
  public final int HEARTBEAT_INTERVAL_MS = 1000;

  private UserConf() {
  }

  public static synchronized UserConf get() {
    if (USER_CONF == null) {
      USER_CONF = new UserConf();
    }

    return USER_CONF;
  }
}
