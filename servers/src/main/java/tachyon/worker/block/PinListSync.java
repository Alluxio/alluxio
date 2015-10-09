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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.WorkerFileSystemMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerContext;

/**
 * PinListSync periodically syncs the set of pinned inodes from master,
 * and saves the new pinned inodes to the BlockDataManager.
 * The syncing parameters (intervals, timeouts) adopt directly from worker-to-master heartbeat
 * configurations.
 *
 */
public final class PinListSync implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block data manager responsible for interacting with Tachyon and UFS storage */
  private final BlockDataManager mBlockDataManager;
  /** Milliseconds between each sync */
  private final int mSyncIntervalMs;
  /** Milliseconds between syncs before a timeout */
  private final int mSyncTimeoutMs;

  /** Client for all master communication */
  private WorkerFileSystemMasterClient mMasterClient;
  /** Flag to indicate if the syncing should continue */
  private volatile boolean mRunning;

  /**
   * Constructor for PinListSync
   *
   * @param blockDataManager the blockDataManager this syncer is updating to
   * @param masterClient the Tachyon master client
   */
  public PinListSync(BlockDataManager blockDataManager, WorkerFileSystemMasterClient masterClient) {
    mBlockDataManager = blockDataManager;
    TachyonConf conf = WorkerContext.getConf();

    mMasterClient = masterClient;
    mSyncIntervalMs = conf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    mSyncTimeoutMs = conf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS);

    mRunning = true;
  }

  /**
   * Main loop for the sync, continuously sync pinlist from master
   */
  @Override
  public void run() {
    long lastSyncMs = System.currentTimeMillis();
    while (mRunning) {
      // Check the time since last sync, and wait until it is within sync interval
      long lastIntervalMs = System.currentTimeMillis() - lastSyncMs;
      long toSleepMs = mSyncIntervalMs - lastIntervalMs;
      if (toSleepMs > 0) {
        CommonUtils.sleepMs(LOG, toSleepMs);
      } else {
        LOG.warn("Sync took: " + lastIntervalMs + ", expected: " + mSyncIntervalMs);
      }

      // Send the sync
      try {
        Set<Long> pinList = mMasterClient.getPinList();
        mBlockDataManager.updatePinList(pinList);
        lastSyncMs = System.currentTimeMillis();
      // TODO(calvin): Change this back to IOException when we have the correct pinlist RPC.
      } catch (Exception ioe) {
        // An error occurred, retry after 1 second or error if sync timeout is reached
        LOG.error("Failed to receive pinlist.", ioe);
        // TODO(gene): Add this method to MasterClientBase.
        // mMasterClient.resetConnection();
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        if (System.currentTimeMillis() - lastSyncMs >= mSyncTimeoutMs) {
          throw new RuntimeException("Master sync timeout exceeded: " + mSyncTimeoutMs);
        }
      }
    }
  }

  /**
   * Stops the syncing, once this method is called, the object should be discarded
   */
  public void stop() {
    mRunning = false;
  }
}
