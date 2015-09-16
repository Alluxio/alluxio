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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.Sessions;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.worker.WorkerContext;

public class AsyncEvictor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockStore mBlockStore;
  private final List<Pair<BlockStoreLocation, Long>> mReservedBytesOnTiers =
      new ArrayList<Pair<BlockStoreLocation, Long>>();
  private final Timer mTimer;
  private final Semaphore mSemaphore = new Semaphore(1);
  private final Thread mEvictorThread;

  public AsyncEvictor(BlockStore blockStore) {
    mBlockStore = blockStore;
    List<Long> capOnTiers = blockStore.getBlockStoreMeta().getCapacityBytesOnTiers();
    List<Integer> aliasOnTiers = blockStore.getBlockStoreMeta().getAliasOnTiers();
    long lastTierReservedBytes = 0;
    for (int idx = 0; idx < aliasOnTiers.size(); idx ++) {
      String tierReservedSpaceProp =
          String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_RESERVED_RATIO_FORMAT, idx);
      int tierAlias = aliasOnTiers.get(idx);
      long reservedSpaceBytes =
          (long)(capOnTiers.get(tierAlias - 1)
              * WorkerContext.getConf().getDouble(tierReservedSpaceProp));
      mReservedBytesOnTiers.add(new Pair<BlockStoreLocation, Long>(
          BlockStoreLocation.anyDirInTier(tierAlias), reservedSpaceBytes + lastTierReservedBytes));
      lastTierReservedBytes += reservedSpaceBytes;
    }
    mEvictorThread = new Thread(this);
    mEvictorThread.setDaemon(true);
    mEvictorThread.setName("async-evictor");
    mTimer = new Timer(true);
  }

  public void initialize() {
    mEvictorThread.start();
    mTimer.schedule(new TimerTask() {
      public void run() {
        mSemaphore.release();
      }
    }, 0L, WorkerContext.getConf().getLong(Constants
        .WORKER_TIERED_STORAGE_EVICTION_ASYNC_PERIOD_MS_FORMAT));
  }

  @Override
  public void run() {
    try {
      while (true) {
        mSemaphore.acquire();
        for (int tierIdx = mReservedBytesOnTiers.size() - 1; tierIdx >= 0 ; tierIdx --) {
          Pair<BlockStoreLocation, Long> bytesReservedOnTier = mReservedBytesOnTiers.get(tierIdx);
          BlockStoreLocation location = bytesReservedOnTier.getFirst();
          long bytesReserved = bytesReservedOnTier.getSecond();
          try {
            mBlockStore.freeSpace(Sessions.DATASERVER_SESSION_ID, bytesReserved, location);
          } catch (OutOfSpaceException e) {
            LOG.warn(e.getMessage());
          } catch (NotFoundException e) {
            LOG.warn(e.getMessage());
          } catch (AlreadyExistsException e) {
            LOG.warn(e.getMessage());
          } catch (InvalidStateException e) {
            LOG.warn(e.getMessage());
          } catch (IOException e) {
            LOG.warn(e.getMessage());
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Asynchronous evictor exits!");
    }
  }
}
