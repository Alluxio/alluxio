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

import tachyon.conf.TachyonConf;

/**
 * Represent one user in the worker daemon.
 */
public class UserInfo {
  private final long mUserId;

  private long mOwnBytes;
  private long mLastHeartbeatMs;

  private final TachyonConf mTachyonConf;

  public UserInfo(long userId, TachyonConf tachyonConf) {
    Preconditions.checkArgument(userId > 0, "Invalid user id " + userId);
    Preconditions.checkArgument(tachyonConf != null, "Cannot pass null for TachyonConf");
    mUserId = userId;
    mOwnBytes = 0;
    mLastHeartbeatMs = System.currentTimeMillis();
    mTachyonConf = tachyonConf;
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
    int userTimeoutMs = mTachyonConf.getInt(Constants.WORKER_USER_TIMEOUT_MS,
        10 * Constants.SECOND_MS);
    return (System.currentTimeMillis() - mLastHeartbeatMs > userTimeoutMs);
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
