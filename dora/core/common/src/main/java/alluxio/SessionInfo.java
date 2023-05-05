/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents one session in the worker daemon.
 */
@ThreadSafe
public class SessionInfo {
  private final long mSessionId;

  private long mLastHeartbeatMs;
  private final int mSessionTimeoutMs;

  /**
   * Creates a new instance of {@link SessionInfo}.
   *
   * @param sessionId the session id
   * @param sessionTimeoutMs the session timeout in milliseconds
   */
  public SessionInfo(long sessionId, int sessionTimeoutMs) {
    Preconditions.checkArgument(sessionId > 0, "Invalid session id " + sessionId);
    Preconditions.checkArgument(sessionTimeoutMs > 0, "Invalid session timeout");
    mSessionId = sessionId;
    mLastHeartbeatMs = System.currentTimeMillis();
    mSessionTimeoutMs = sessionTimeoutMs;
  }

  /**
   * @return the session id
   */
  public long getSessionId() {
    return mSessionId;
  }

  /**
   * Performs a session heartbeat.
   */
  public synchronized void heartbeat() {
    mLastHeartbeatMs = System.currentTimeMillis();
  }

  /**
   * Checks whether the session has timed out.
   *
   * @return true if the session has timed out and false otherwise
   */
  public synchronized boolean timeout() {
    return (System.currentTimeMillis() - mLastHeartbeatMs > mSessionTimeoutMs);
  }

  @Override
  public synchronized String toString() {
    return MoreObjects.toStringHelper(this).add("sessionId", mSessionId)
        .add("lastHeartbeatMs", mLastHeartbeatMs).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SessionInfo)) {
      return false;
    }
    SessionInfo that = (SessionInfo) o;
    return mSessionId == that.mSessionId;
  }

  @Override
  public int hashCode() {
    return (int) (mSessionId ^ (mSessionId >>> 32));
  }
}
