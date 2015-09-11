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

package tachyon;

import com.google.common.base.Preconditions;

/**
 * Represent one session in the worker daemon.
 */
public class SessionInfo {
  private final long mSessionId;

  private long mLastHeartbeatMs;
  private int mSessionTimeoutMs;

  public SessionInfo(long sessionId, int sessionTimeoutMs) {
    Preconditions.checkArgument(sessionId > 0, "Invalid session id " + sessionId);
    Preconditions.checkArgument(sessionTimeoutMs > 0, "Invalid session timeout");
    mSessionId = sessionId;
    mLastHeartbeatMs = System.currentTimeMillis();
    mSessionTimeoutMs = sessionTimeoutMs;
  }

  public long getSessionId() {
    return mSessionId;
  }

  public synchronized void heartbeat() {
    mLastHeartbeatMs = System.currentTimeMillis();
  }

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
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SessionInfo sessionInfo = (SessionInfo) o;
    return mSessionId == sessionInfo.mSessionId;
  }

  @Override
  public int hashCode() {
    return (int) (mSessionId ^ (mSessionId >>> 32));
  }
}
