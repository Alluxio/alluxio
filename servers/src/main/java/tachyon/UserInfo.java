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
 * Represent one user in the worker daemon.
 */
public class UserInfo {
  private final long mUserId;

  private long mLastHeartbeatMs;
  private int mUserTimeoutMs;

  public UserInfo(long userId, int userTimeoutMs) {
    Preconditions.checkArgument(userId > 0, "Invalid user id " + userId);
    Preconditions.checkArgument(userTimeoutMs > 0, "Invalid user timeout");
    mUserId = userId;
    mLastHeartbeatMs = System.currentTimeMillis();
    mUserTimeoutMs = userTimeoutMs;
  }

  public long getUserId() {
    return mUserId;
  }

  public synchronized void heartbeat() {
    mLastHeartbeatMs = System.currentTimeMillis();
  }

  public synchronized boolean timeout() {
    return (System.currentTimeMillis() - mLastHeartbeatMs > mUserTimeoutMs);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("UserInfo(");
    sb.append(" mUserId: ").append(mUserId);
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
    UserInfo userInfo = (UserInfo) o;
    return mUserId == userInfo.mUserId;
  }

  @Override
  public int hashCode() {
    return (int) (mUserId ^ (mUserId >>> 32));
  }
}
