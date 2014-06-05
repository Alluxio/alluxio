/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;

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
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("UserInfo(");
    sb.append(" USER_ID: ").append(USER_ID);
    sb.append(", mOwnBytes: ").append(mOwnBytes);
    sb.append(", mLastHeartbeatMs: ").append(mLastHeartbeatMs);
    sb.append(")");
    return sb.toString();
  }
}