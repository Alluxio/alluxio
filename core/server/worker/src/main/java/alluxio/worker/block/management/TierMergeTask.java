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
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.order.BlockOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A BlockStore management task that is to merge blocks across tiers in order to eliminate
 * overlaps according to eviction order scheme. Overlap elimination works by moving blocks.
 *
 * A single task may not be enough to completely eliminate the overlap,
 * so {@link ManagementTaskCoordinator} will keep instantiating new merge tasks until
 * an instantiated task reports negative for a need to run.
 */
public class TierMergeTask extends AbstractBlockManagementTask {
  private static final Logger LOG = LoggerFactory.getLogger(TierMergeTask.class);

  /**
   * Creates a new merge task.
   *
   * @param blockStore block store
   * @param metadataManager meta manager
   * @param evictorView evictor view
   * @param loadTracker load tracker
   */
  public TierMergeTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker) {
    super(blockStore, metadataManager, evictorView, loadTracker);
  }

  @Override
  public void run() {
    // Acquire merge range from configuration. This will limit move operations in single task run.
    final int maxIntersectionWidth =
        ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_MERGE_MAX_INTERSECTION_WIDTH);

    // Iterate each tier intersection and merge overlaps.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLocation = intersection.getFirst();
      BlockStoreLocation tierDownLocation = intersection.getSecond();

      // Get sorted iterator for the current intersection.
      List<Long> intersectionList =
          mMetadataManager.getBlockIterator().getIntersectionList(tierUpLocation,
              BlockOrder.Natural, tierDownLocation, BlockOrder.Reverse, maxIntersectionWidth,
              BlockOrder.Reverse, (blockId) -> !mEvictorView.isBlockEvictable(blockId));

      // TODO(ggezer): TV2 - Implement BlockStore.swapBlocks() to simplify the logic here.
      // TODO(ggezer): TV2 - Allocate before move in order to back-off for exact destinations.

      /**
       * Overlap will be attempted to be inserted back in sorted order.
       * Insertion will start from upper tier as long as there is space.
       * This will make sure blocks go up when there is space in upper tier.
       */
      BlockStoreLocation moveLocation = tierUpLocation;
      int currentBlockIdx = 0;
      while (currentBlockIdx < intersectionList.size()) {
        if (Thread.interrupted()) {
          LOG.warn("Tier merge task interrupted.");
          return;
        }
        // {@link ManagementTaskCoordinator} backs off on any load.
        // This, combined with the logic here, means if a merge task finds a chance to run,
        // It will back-off only when the tiers it's working on gets busy.
        if (mLoadTracker.loadDetected(tierUpLocation)
            || mLoadTracker.loadDetected(tierDownLocation)) {
          // Stop merging if load detected on tiers being merged.
          LOG.warn("Stopping merge task due to user activity.");
          return;
        }

        // Grab current block to place.
        Long blockId = intersectionList.get(currentBlockIdx);
        try {
          // This might be a noop in the store, if the block already in target location.
          mBlockStore.moveBlock(Sessions.createInternalSessionId(), blockId, moveLocation);
          currentBlockIdx++;
        } catch (WorkerOutOfSpaceException e) {
          // Current write tier went out of space.
          // Break if it was the tier down.
          if (moveLocation.equals(tierDownLocation)) {
            break;
          }
          // Continue moving it to tier down.
          moveLocation = tierDownLocation;
        } catch (Exception e) {
          LOG.warn("Move failed during tier-merge for block: {}. Error: {}", blockId, e);
          currentBlockIdx++;
        }
      }
      LOG.info("Merging task completed between {} - {}. Merge range: {}", tierUpLocation,
          tierDownLocation, intersectionList.size());
    }
  }
}
