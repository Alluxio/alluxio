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
import alluxio.PropertyKey;
import alluxio.PropertyKeyFormat;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.heartbeat.HeartbeatExecutor;

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
public class SpaceReserver implements HeartbeatExecutor  {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceReserver.class);
  private final BlockWorker mBlockWorker;

  /** Association between storage tier aliases and ordinals for the worker. */
  private final StorageTierAssoc mStorageTierAssoc;

  /** Mapping from tier alias to high watermark in bytes. */
  private final Map<String, Long> mHighWaterMarkInBytesOnTiers = new HashMap<>();

  /** Mapping from tier alias to space size to be reserved on the tier. */
  private final Map<String, Long> mLowWaterMarkInBytesOnTiers = new HashMap<>();

  /**
   * Creates a new instance of {@link SpaceReserver}.
   *
   * @param blockWorker the block worker handle
   */
  public SpaceReserver(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
    mStorageTierAssoc = new WorkerStorageTierAssoc();
    Map<String, Long> capOnTiers = blockWorker.getStoreMeta().getCapacityBytesOnTiers();
    for (int ordinal = 0; ordinal < mStorageTierAssoc.size(); ordinal++) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      // HighWatemark defines when to start the space reserving process
      PropertyKey tierHighWatermarkProp =
          PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_HIGH_WATERMARK_RATIO_FORMAT.format(ordinal);
      long highWatermarkInBytes =
          (long) (capOnTiers.get(tierAlias) * Configuration.getDouble(tierHighWatermarkProp));

      // LowWatemark defines when to stop the space reserving process if started
      PropertyKey tierLowWatermarkProp =
          PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_LOW_WATERMARK_RATIO_FORMAT.format(ordinal);
      long capOnTier = capOnTiers.get(tierAlias);
      long lowWatermarkInBytes =
          (long) (capOnTier - capOnTier * Configuration.getDouble(tierLowWatermarkProp));
      mHighWaterMarkInBytesOnTiers.put(tierAlias, highWatermarkInBytes);
      mLowWaterMarkInBytesOnTiers.put(tierAlias, lowWatermarkInBytes);
    }
  }

  private void reserveSpace() {
    Map<String, Long> usedBytesOnTiers = mBlockWorker.getStoreMeta().getUsedBytesOnTiers();
    for (int ordinal = 0; ordinal < mStorageTierAssoc.size(); ordinal++) {
      String tierAlias = mStorageTierAssoc.getAlias(ordinal);
      long highWatermarkInBytes = mHighWaterMarkInBytesOnTiers.get(tierAlias);
      if (highWatermarkInBytes > 0 && usedBytesOnTiers.get(tierAlias) >= highWatermarkInBytes) {
        long lowWatermarkInBytes = mLowWaterMarkInBytesOnTiers.get(tierAlias);
        try {
          mBlockWorker.freeSpace(Sessions.MIGRATE_DATA_SESSION_ID, lowWatermarkInBytes, tierAlias);
        } catch (WorkerOutOfSpaceException | BlockDoesNotExistException
            | BlockAlreadyExistsException | InvalidWorkerStateException | IOException e) {
          LOG.warn("SpaceReserver failed to free tier {} to {} bytes used",
              tierAlias, lowWatermarkInBytes, e);
        }
      }
    }
  }

  @Override
  public void heartbeat() {
    reserveSpace();
  }

  @Override
  public void close() {
    // Nothing to close.
  }
}
