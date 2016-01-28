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

package tachyon.worker.block;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerContext;

/**
 * SessionCleaner periodically checks if any session have become zombies, removes the zombie session
 * and associated data when necessary. The syncing parameters (intervals) adopt directly from
 * worker-to-master heartbeat configurations.
 */
public final class SessionCleaner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Block worker handler responsible for interacting with Tachyon and UFS storage */
  private final BlockWorker mBlockWorker;
  /** Milliseconds between each check */
  private final int mCheckIntervalMs;

  /** Flag to indicate if the checking should continue */
  private volatile boolean mRunning;

  /**
   * Creates a new instance of {@link SessionCleaner}.
   *
   * @param blockWorker the block worker handle
   */
  public SessionCleaner(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
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
      mBlockWorker.cleanupSessions();
    }
  }

  /**
   * Stops the checking, once this method is called, the object should be discarded
   */
  public void stop() {
    mRunning = false;
  }
}
