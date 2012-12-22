package tachyon;

public class UserInfo {
  private long mUserId;
  private long mOwnBytes;
  private long mLastHeartbeatMs;

  public UserInfo(long userId) {
    if (userId <= 0) {
      CommonUtils.runtimeException("Invalid user id " + userId);
    }
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

  public synchronized long getUserId() {
    return mUserId;
  }

  public synchronized void heartbeat() {
    mLastHeartbeatMs = System.currentTimeMillis();
  }

  public synchronized boolean timeout() {
    return (System.currentTimeMillis() - mLastHeartbeatMs > Config.USER_TIMEOUT_MS); 
  }
}