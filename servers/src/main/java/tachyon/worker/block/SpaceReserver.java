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
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.StorageTierAssoc;
import tachyon.Sessions;
import tachyon.WorkerStorageTierAssoc;
import tachyon.exception.BlockAlreadyExistsException;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.exception.WorkerOutOfSpaceException;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerContext;

/**
 * SpaceReserver periodically checks if there is enough space reserved on each storage tier, if
 * there is no enough free space on some tier, free space from it.
 */
public class SpaceReserver implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockDataManager mBlockManager;
  /** Association between storage tier aliases and ordinals for the worker */
  private final StorageTierAssoc mStorageTierAssoc;
  /** Mapping from tier alias to high watermark in bytes */
  private final Map<String, Long> mHighWaterMarkInBytesOnTiers = new HashMap<String, Long>();
  /** Mapping from tier alias to space size to be reserved on the tier */
  private final Map<String, Long> mBytesToReserveOnTiers = new HashMap<String, Long>();
  /** Milliseconds between each check */
  private final int mCheckIntervalMs;
  /** Flag to indicate if the checking should continue */
  private volatile boolean mRunning;

  public SpaceReserver(BlockDataManager blockManager) {
    mBlockManager = blockManager;
    mStorageTierAssoc = new WorkerStorageTierAssoc(WorkerContext.getConf());
    Map<String, Long> capOnTiers = mBlockManager.getStoreMeta().getCapacityBytesOnTiers();
    long lastTierReservedBytes = 0;
    for (int ordinal = 0; ordinal < mStorageTierAssoc.size(); ordinal ++) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      // To-do: make sure highWatermark ratio is larger than lowWatermark ratio
      // HighWatemark defines when to start the space reserving process
      String highWatermarkRatio =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO_FORMAT, ordinal);
      long highWatermarkInBytes =
          (long) (capOnTiers.get(tierAlias) * WorkerContext.getConf().getDouble(
              highWatermarkRatio));
      mHighWaterMarkInBytesOnTiers.put(tierAlias, highWatermarkInBytes);

      // LowWatemark defines when to stop the space reserving process if started
      String lowWatermarkRatio =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO_FORMAT, ordinal);
      long reservedSpaceBytes =
          (long) (capOnTiers.get(tierAlias)
              * (1 - WorkerContext.getConf().getDouble(lowWatermarkRatio)));
      mBytesToReserveOnTiers.put(tierAlias, reservedSpaceBytes + lastTierReservedBytes);
      lastTierReservedBytes += reservedSpaceBytes;
    }
    mCheckIntervalMs =
        WorkerContext.getConf().getInt(Constants.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS);
    mRunning = true;
  }

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
        LOG.warn("Space reserver took: {}, expected: {}", lastIntervalMs, mCheckIntervalMs);
      }
      reserveSpace();
    }
  }

  /**
   * Stops the checking, once this method is called, the object should be discarded
   */
  public void stop() {
    LOG.info("Space reserver exits!");
    mRunning = false;
  }

  private void reserveSpace() {
    Map<String, Long> usedBytesOnTiers = mBlockManager.getStoreMeta().getUsedBytesOnTiers();
    for (int ordinal = mStorageTierAssoc.size() - 1; ordinal >= 0 ; ordinal --) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long highWatermarkInBytes = mHighWaterMarkInBytesOnTiers.get(tierAlias);
      if (highWatermarkInBytes > 0 && usedBytesOnTiers.get(tierAlias) >= highWatermarkInBytes) {
        long bytesReserved = mBytesToReserveOnTiers.get(tierAlias);
        try {
          mBlockManager.freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, bytesReserved, tierAlias);
        } catch (WorkerOutOfSpaceException e) {
          LOG.warn(e.getMessage());
        } catch (BlockDoesNotExistException e) {
          LOG.warn(e.getMessage());
        } catch (BlockAlreadyExistsException e) {
          LOG.warn(e.getMessage());
        } catch (InvalidWorkerStateException e) {
          LOG.warn(e.getMessage());
        } catch (IOException e) {
          LOG.warn(e.getMessage());
        }
      }
    }
  }
}
