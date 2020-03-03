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
import alluxio.worker.block.order.BlockOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A BlockStore management task that is to swap blocks between tiers in order to eliminate overlaps
 * according to eviction order scheme.
 *
 * A single task may not be enough to completely eliminate the overlap, so
 * {@link ManagementTaskCoordinator} will keep instantiating new swap tasks
 * until no longer needed.
 */
public class TierSwapTask extends AbstractBlockManagementTask {
  private static final Logger LOG = LoggerFactory.getLogger(TierSwapTask.class);

  /**
   * Creates a new swap task.
   *
   * @param blockStore block store
   * @param metadataManager meta manager
   * @param evictorView evictor view
   * @param loadTracker load tracker
   */
  public TierSwapTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker) {
    super(blockStore, metadataManager, evictorView, loadTracker);
  }

  @Override
  public void run() {
    // Acquire swap range from the configuration.
    // This will limit swap operations in single task run.
    final int swapRange = ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_SWAP_RANGE);

    // Iterate each tier intersection and avoid overlaps by swapping blocks.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLocation = intersection.getFirst();
      BlockStoreLocation tierDownLocation = intersection.getSecond();

      // Get list of swaps required to eliminate the overlap.
      List<Pair<Long, Long>> swapList = mMetadataManager.getBlockIterator().getSwaps(tierUpLocation,
          BlockOrder.Natural, tierDownLocation, BlockOrder.Reverse, swapRange,
          BlockOrder.Reverse, (blockId) -> !mEvictorView.isBlockEvictable(blockId));

      // Execute swaps.
      // TODO(ggezer): TV2 - Make it parallel by inspecting parent directories.
      for (Pair<Long, Long> swap : swapList) {
        // {@link ManagementTaskCoordinator} backs off on any load.
        // This, combined with the logic here, means if a swap task finds a chance to run,
        // It will back-off only when the tiers it's working on gets busy.
        if (mLoadTracker.loadDetected(tierUpLocation)
            || mLoadTracker.loadDetected(tierDownLocation)) {
          // Stop swapping if load detected on current tier intersection.
          LOG.warn("Stopping tier-swap task due to user activity.");
          return;
        }

        try {
          mBlockStore.swapBlocks(Sessions.createInternalSessionId(),
              swap.getFirst(), swap.getSecond());
        } catch (Exception e) {
          LOG.warn("Swapping blocks {}-{} failed with error: {}", swap.getFirst(), swap.getSecond(),
              e);
        }
      }
    }
  }
}
