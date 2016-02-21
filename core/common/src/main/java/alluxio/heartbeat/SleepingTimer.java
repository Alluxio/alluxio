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

package alluxio.heartbeat;

import alluxio.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class can be used for executing heartbeats periodically.
 */
@NotThreadSafe
public final class SleepingTimer implements HeartbeatTimer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final long mIntervalMs;
  private long mPreviousTickMs;
  private final String mThreadName;

  /**
   * Creates a new instance of {@link SleepingTimer}.
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval
   */
  public SleepingTimer(String threadName, long intervalMs) {
    mIntervalMs = intervalMs;
    mThreadName = threadName;
  }

  /**
   * Enforces the thread waits for the given interval between consecutive ticks.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public void tick() throws InterruptedException {
    if (mPreviousTickMs == 0) {
      Thread.sleep(mIntervalMs);
    } else {
      long executionTimeMs = System.currentTimeMillis() - mPreviousTickMs;
      if (executionTimeMs > mIntervalMs) {
        LOG.warn("{} last execution took {} ms. Longer than the interval {}",
                mThreadName, executionTimeMs, mIntervalMs);
      } else {
        Thread.sleep(mIntervalMs - executionTimeMs);
      }
    }
    mPreviousTickMs = System.currentTimeMillis();
  }
}
