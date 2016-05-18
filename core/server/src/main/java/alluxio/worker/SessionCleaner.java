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

package alluxio.worker;

import alluxio.Constants;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * SessionCleaner periodically checks if any session have become zombies, removes the zombie session
 * and associated data when necessary. The syncing parameters (intervals) adopt directly from
 * worker-to-master heartbeat configurations.
 */
@NotThreadSafe
public final class SessionCleaner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The object which supports cleaning up sessions. */
  private final SessionCleanupCallback mSessionCleanupCallback;
  /** Milliseconds between each check. */
  private final int mCheckIntervalMs;

  /** Flag to indicate if the checking should continue. */
  private volatile boolean mRunning;

  /**
   * Creates a new instance of {@link SessionCleaner}.
   *
   * @param sessionCleanupCallback the session clean up callback which will periodically be invoked
   */
  public SessionCleaner(SessionCleanupCallback sessionCleanupCallback) {
    mSessionCleanupCallback = sessionCleanupCallback;
    mCheckIntervalMs =
        WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);

    mRunning = true;
  }

  /**
   * Main loop for the cleanup, continuously looks for zombie sessions.
   */
  @Override
  public void run() {
    long lastCheckMs = System.currentTimeMillis();
    while (mRunning) {
      // Check the time since last check, and wait until it is within check interval
      long lastIntervalMs = System.currentTimeMillis() - lastCheckMs;
      long toSleepMs = mCheckIntervalMs - lastIntervalMs;
      if (toSleepMs > 0) {
        CommonUtils.sleepMs(LOG, toSleepMs);
      } else {
        LOG.warn("Session cleanup took: {}, expected: {}", lastIntervalMs, mCheckIntervalMs);
      }

      // Check if any sessions have become zombies, if so clean them up
      lastCheckMs = System.currentTimeMillis();
      mSessionCleanupCallback.cleanupSessions();
    }
  }

  /**
   * Stops the checking, once this method is called, the object should be discarded.
   */
  public void stop() {
    mRunning = false;
  }
}
