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

package alluxio.worker.block.management;

import alluxio.Sessions;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.order.BlockOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A BlockStore management task that is to move blocks to higher tiers.
 * This is to ensure faster tiers utilized to increase overall performance.
 *
 * A single task may not be enough to complete the move, so {@link ManagementTaskCoordinator}
 * will keep instantiating new move tasks until no longer needed.
 */
public class TierMoveTask extends AbstractBlockManagementTask {
  private static final Logger LOG = LoggerFactory.getLogger(TierMoveTask.class);

  /**
   * Creates a new move task.
   *
   * @param blockStore block store
   * @param metadataManager meta manager
   * @param evictorView evictor view
   * @param loadTracker load tracker
   */
  public TierMoveTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker) {
    super(blockStore, metadataManager, evictorView, loadTracker);
  }

  @Override
  public void run() {
    // Acquire move range from the configuration.
    // This will limit move operations in single task run.
    final int moveRange = ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_RANGE);

    // Iterate each tier intersection and merge overlaps.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLocation = intersection.getFirst();
      BlockStoreLocation tierDownLocation = intersection.getSecond();

      // Acquire iterator for the tier below.
      Iterator<Long> tierDownIterator =
          mMetadataManager.getBlockIterator().getIterator(tierDownLocation, BlockOrder.Reverse);

      // Execute moves.
      // TODO(ggezer): TV2 - Make it parallel by inspecting parent directories.
      int movedCount = 0;
      while (tierDownIterator.hasNext() && movedCount++ < moveRange) {
        // {@link ManagementTaskCoordinator} backs off on any load.
        // This, combined with the logic here, means if a move task finds a chance to run,
        // It will back-off only when the tiers it's working on gets busy.
        if (mLoadTracker.loadDetected(tierUpLocation)
            || mLoadTracker.loadDetected(tierDownLocation)) {
          // Stop moving if load detected on current tier intersection.
          LOG.warn("Stopping merge task due to user activity.");
          return;
        }

        // Stop moving if reached maximum allowed space on higher tier.
        StorageTier tierUp = mMetadataManager.getTier(tierUpLocation.tierAlias());
        double currentFreeSpace = (double) tierUp.getAvailableBytes() / tierUp.getCapacityBytes();
        if (currentFreeSpace <= ServerConfiguration
            .getDouble(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_LIMIT)) {
          LOG.info("Tier move task stopping at configured free space for tiers.");
          break;
        }

        // Move the current block up.
        long nextBlockId = tierDownIterator.next();
        try {
          mBlockStore.moveBlock(Sessions.createInternalSessionId(), nextBlockId, tierUpLocation);
        } catch (Exception e) {
          LOG.warn("Move failed during tier-move for block: {}. Error: {}", nextBlockId, e);
        }
      }
    }
  }
}
