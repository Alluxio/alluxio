package tachyon;

import com.google.common.base.Preconditions;

import tachyon.conf.WorkerConf;

/**
 * Represent one user in the worker daemon.
 */
public class UserInfo {
  private final long mUserId;

  private long mOwnBytes;
  private long mLastHeartbeatMs;

  public UserInfo(long userId) {
    Preconditions.checkArgument(userId > 0, "Invalid user id " + userId);
    mUserId = userId;
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
    return mUserId;
  }

  public synchronized void heartbeat() {
    mLastHeartbeatMs = System.currentTimeMillis();
  }

  public synchronized boolean timeout() {
    return (System.currentTimeMillis() - mLastHeartbeatMs > WorkerConf.get().USER_TIMEOUT_MS);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("UserInfo(");
    sb.append(" mUserId: ").append(mUserId);
    sb.append(", mOwnBytes: ").append(mOwnBytes);
    sb.append(", mLastHeartbeatMs: ").append(mLastHeartbeatMs);
    sb.append(")");
    return sb.toString();
  }
}
