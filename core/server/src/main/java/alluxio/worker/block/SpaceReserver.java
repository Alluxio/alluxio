/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * {@link SpaceReserver} periodically checks if there is enough space reserved on each storage tier,
 * if there is no enough free space on some tier, free space from it.
 */
@NotThreadSafe
public class SpaceReserver implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockWorker mBlockWorker;

  /** Association between storage tier aliases and ordinals for the worker. */
  private final StorageTierAssoc mStorageTierAssoc;

  /** Mapping from tier alias to space size to be reserved on the tier. */
  private final Map<String, Long> mBytesToReserveOnTiers = new HashMap<>();

  /** Milliseconds between each check. */
  private final int mCheckIntervalMs;

  /** Flag to indicate if the checking should continue. */
  private volatile boolean mRunning;

  /**
   * Creates a new instance of {@link SpaceReserver}.
   *
   * @param blockWorker the block worker handle
   */
  public SpaceReserver(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
    mStorageTierAssoc = new WorkerStorageTierAssoc();
    Map<String, Long> capOnTiers = blockWorker.getStoreMeta().getCapacityBytesOnTiers();
    long lastTierReservedBytes = 0;
    for (int ordinal = 0; ordinal < mStorageTierAssoc.size(); ordinal++) {
      String tierReservedSpaceProp =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, ordinal);
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long reservedSpaceBytes =
          (long) (capOnTiers.get(tierAlias) * Configuration.getDouble(tierReservedSpaceProp));
      mBytesToReserveOnTiers.put(tierAlias, reservedSpaceBytes + lastTierReservedBytes);
      lastTierReservedBytes += reservedSpaceBytes;
    }
    mCheckIntervalMs = Configuration.getInt(Constants.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS);
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
      lastCheckMs = System.currentTimeMillis();
      reserveSpace();
    }
  }

  /**
   * Stops the checking, once this method is called, the object should be discarded.
   */
  public void stop() {
    LOG.info("Space reserver exits!");
    mRunning = false;
  }

  private void reserveSpace() {
    for (int ordinal = mStorageTierAssoc.size() - 1; ordinal >= 0; ordinal--) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long bytesReserved = mBytesToReserveOnTiers.get(tierAlias);
      try {
        mBlockWorker.freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, bytesReserved, tierAlias);
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
