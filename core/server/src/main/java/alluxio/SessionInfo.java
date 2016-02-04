/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

/**
 * Represent one session in the worker daemon.
 */
@ThreadSafe
public class SessionInfo {
  private final long mSessionId;

  private long mLastHeartbeatMs;
  private int mSessionTimeoutMs;

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
    StringBuilder sb = new StringBuilder("SessionInfo(");
    sb.append(" mSessionId: ").append(mSessionId);
    sb.append(", mLastHeartbeatMs: ").append(mLastHeartbeatMs);
    sb.append(")");
    return sb.toString();
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
