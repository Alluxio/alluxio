package tachyon;

/**
 * Heartbeat executor for master client.
 */
class MasterClientHeartbeatExecutor implements HeartbeatExecutor {
  private final MasterClient CLIENT;
  private final long MAX_NONE_ACCESS_INTERVAL;

  public MasterClientHeartbeatExecutor(MasterClient client, long maxNoneAccessIntervalMs) {
    CLIENT = client;
    MAX_NONE_ACCESS_INTERVAL = maxNoneAccessIntervalMs;
  }

  @Override
  public void heartbeat() {
    if (System.currentTimeMillis() - CLIENT.getLastAccessedMs() > MAX_NONE_ACCESS_INTERVAL) {
      CLIENT.close();
    }
  }
}
