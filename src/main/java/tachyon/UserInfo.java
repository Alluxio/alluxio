package tachyon;

import tachyon.conf.WorkerConf;

/**
 * Represent one user in the worker daemon. 
 */
public class UserInfo {
  private final long USER_ID;

  private long mOwnBytes;
  private long mLastHeartbeatMs;

  public UserInfo(long userId) {
    if (userId <= 0) {
      CommonUtils.runtimeException("Invalid user id " + userId);
    }
    USER_ID = userId;
    mOwnBytes = 0;
    mLastHeartbeatMs = System.currentTimeMillis();
  }

  public synchronized void addOwnBytes(long addOwnBytes) {
    mOwnBytes += addOwnBytes;
  }

  public synchronized long getOwnBytes() {
    return mOwnBytes;
  }

  public long getUserId() {
    return USER_ID;
  }

  public synchronized void heartbeat() {
    mLastHeartbeatMs = System.currentTimeMillis();
  }

  public synchronized boolean timeout() {
    return (System.currentTimeMillis() - mLastHeartbeatMs > WorkerConf.get().USER_TIMEOUT_MS); 
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("UserInfo(");
    sb.append(" USER_ID: ").append(USER_ID);
    sb.append(", mOwnBytes: ").append(mOwnBytes);
    sb.append(", mLastHeartbeatMs: ").append(mLastHeartbeatMs);
    sb.append(")");
    return sb.toString();
  }
}