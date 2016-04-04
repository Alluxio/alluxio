/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
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
