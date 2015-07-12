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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.util.CommonUtils;
import tachyon.util.ThreadFactoryUtils;

/**
 * PinListSync periodically syncs the set of pinned inodes from master,
 * and save the new pinned inodes to the BlockDataManager.
 * The syncing parameters (intervals, timeouts) adopt directly from worker-to-master heartbeat
 * configurations.
 *
 */
public class PinListSync implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block data manager responsible for interacting with Tachyon and UFS storage */
  private final BlockDataManager mBlockDataManager;
  /** The executor service for the master client thread */
  private final ExecutorService mMasterClientExecutorService;
  /** The configuration values */
  private final TachyonConf mTachyonConf;
  /** Milliseconds between each sync */
  private final int mSyncIntervalMs;
  /** Milliseconds between syncs before a timeout */
  private final int mSyncTimeoutMs;

  /** Client for all master communication */
  private MasterClient mMasterClient;
  /** Flag to indicate if the syncing should continue */
  private volatile boolean mRunning;

  /**
   * Constructor for PinListSync
   *
   * @param blockDataManager the blockDataManager this syncer is updating to
   * @param tachyonConf the configuration values to be used
   * @return PinListSync constructed
   */
  public PinListSync(BlockDataManager blockDataManager, TachyonConf tachyonConf) {
    mBlockDataManager = blockDataManager;
    mTachyonConf = tachyonConf;
    mMasterClientExecutorService =
        Executors.newFixedThreadPool(1,
            ThreadFactoryUtils.build("worker-client-pinlist-%d", true));
    mMasterClient =
        new MasterClient(BlockWorkerUtils.getMasterAddress(mTachyonConf),
            mMasterClientExecutorService, mTachyonConf);
    mSyncIntervalMs =
        mTachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS);
    mSyncTimeoutMs =
        mTachyonConf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS, 10 * Constants.SECOND_MS);

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
        Set<Integer> pinList = mMasterClient.worker_getPinIdList();
        mBlockDataManager.updatePinList(pinList);
        lastSyncMs = System.currentTimeMillis();
      } catch (IOException ioe) {
        // An error occurred, retry after 1 second or error if sync timeout is reached
        LOG.error("Failed to receive pinlist.", ioe);
        resetMasterClient();
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
    mMasterClient.close();
    mMasterClientExecutorService.shutdown();
  }

  /**
   * Closes and creates a new master client, in case the master changes.
   */
  private void resetMasterClient() {
    mMasterClient.close();
    mMasterClient =
        new MasterClient(BlockWorkerUtils.getMasterAddress(mTachyonConf),
            mMasterClientExecutorService, mTachyonConf);
  }
}
