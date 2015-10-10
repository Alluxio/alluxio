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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.collections.Pair;
import tachyon.Sessions;
import tachyon.exception.BlockAlreadyExistsException;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.exception.WorkerOutOfSpaceException;
import tachyon.test.Testable;
import tachyon.test.Tester;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerContext;

/**
 * SpaceReserver periodically checks if there is enough space reserved on each storage tier, if
 * there is no enough free space on some tier, free space from it.
 */
public class SpaceReserver implements Runnable, Testable<SpaceReserver> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockDataManager mBlockManager;
  /** Mapping from tier alias to space size to be reserved on the tier */
  private final List<Pair<Integer, Long>> mBytesToReserveOnTiers =
      new ArrayList<Pair<Integer, Long>>();
  /** Milliseconds between each check */
  private final int mCheckIntervalMs;
  /** Flag to indicate if the checking should continue */
  private volatile boolean mRunning;

  public SpaceReserver(BlockDataManager blockManager) {
    mBlockManager = blockManager;
    List<Long> capOnTiers = blockManager.getStoreMeta().getCapacityBytesOnTiers();
    List<Integer> aliasOnTiers = blockManager.getStoreMeta().getAliasOnTiers();
    long lastTierReservedBytes = 0;
    for (int idx = 0; idx < aliasOnTiers.size(); idx ++) {
      String tierReservedSpaceProp =
          String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_RESERVED_RATIO_FORMAT, idx);
      int tierAlias = aliasOnTiers.get(idx);
      /** Similar to {@link BlockStoreMeta}, the alias index is the value of alias - 1 */
      long reservedSpaceBytes =
          (long)(capOnTiers.get(tierAlias - 1) * WorkerContext.getConf()
              .getDouble(tierReservedSpaceProp));
      mBytesToReserveOnTiers.add(new Pair<Integer, Long>(tierAlias,
          reservedSpaceBytes + lastTierReservedBytes));
      lastTierReservedBytes += reservedSpaceBytes;
    }
    mCheckIntervalMs =
        WorkerContext.getConf().getInt(Constants.WORKER_SPACE_RESERVER_INTERVAL_MS);
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
        LOG.warn("Space reserver took: " + lastIntervalMs + ", expected: " + mCheckIntervalMs);
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

  @Override
  public void grantAccess(Tester<SpaceReserver> tester) {
    tester.receiveAccess(new PrivateAccess());
  }

  private void reserveSpace() {
    for (int tierIdx = mBytesToReserveOnTiers.size() - 1; tierIdx >= 0 ; tierIdx --) {
      Pair<Integer, Long> bytesReservedOnTier = mBytesToReserveOnTiers.get(tierIdx);
      int tierAlias = bytesReservedOnTier.getFirst();
      long bytesReserved = bytesReservedOnTier.getSecond();
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

  /** Grants access to private members to testers of this class. */
  class PrivateAccess {
    private PrivateAccess() {}

    public void reserveSpace() {
      SpaceReserver.this.reserveSpace();
    }
  }
}
