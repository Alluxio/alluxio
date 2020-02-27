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
import java.util.function.Supplier;

/**
 * {@link ManagementTaskProvider} implementation for tier management tasks.
 *
 * Currently it produces only 1 type of task {@link TierMergeTask} for: - Maintaining layering based
 * on access pattern - Promoting blocks
 */
public class TierManagementTaskProvider implements ManagementTaskProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TierManagementTaskProvider.class);

  private final BlockStore mBlockStore;
  private final BlockMetadataManager mMetadataManager;
  private final Supplier<BlockMetadataEvictorView> mEvictorViewSupplier;
  private final StoreLoadTracker mLoadTracker;

  /**
   * Creates a task provider for tier management functions.
   *
   * @param blockStore block store
   * @param metadataManager meta manager
   * @param evictorViewSupplier evictor view supplier
   * @param loadTracker load tracker
   */
  public TierManagementTaskProvider(BlockStore blockStore, BlockMetadataManager metadataManager,
      Supplier<BlockMetadataEvictorView> evictorViewSupplier, StoreLoadTracker loadTracker) {
    mBlockStore = blockStore;
    mMetadataManager = metadataManager;
    mEvictorViewSupplier = evictorViewSupplier;
    mLoadTracker = loadTracker;
  }

  @Override
  public BlockManagementTask getTask() {
    if (!ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_MERGE_ENABLED)) {
      return null;
    }

    if (needMerging()) {
      return new TierMergeTask(mBlockStore, mMetadataManager, mEvictorViewSupplier.get(),
          mLoadTracker);
    }

    return null;
  }

  /**
   * @return {@code false} if there are no overlaps between any tier intersection
   */
  private boolean needMerging() {
    // Acquire a recent evictor view.
    BlockMetadataEvictorView evictorView = mEvictorViewSupplier.get();
    // Iterate all tier intersections for deciding whether to run a merge.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {

      // Check if needs merging due to an overlap.
      if (mMetadataManager.getBlockIterator().overlaps(intersection.getFirst(),
          intersection.getSecond(), BlockOrder.Natural,
          (blockId) -> !evictorView.isBlockEvictable(blockId))) {

        LOG.debug("Needs merging due to an overlap between: {} - {}", intersection.getFirst(),
            intersection.getSecond());

        // TODO(ggezer): TV2 - Remove after swap support.
        // Make sure merging can make progress.
        long blockSize = ServerConfiguration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
        if (evictorView.getAvailableBytes(intersection.getFirst()) < blockSize
            && evictorView.getAvailableBytes(intersection.getSecond()) < blockSize) {
          LOG.debug("Overlap can't be eliminated due to lack of block swap support");
          return false;
        }
        return true;
      }

      // Check if needs merging due to promotions.
      StorageTier highTier = mMetadataManager.getTier(intersection.getFirst().tierAlias());
      double currentFreeSpace = (double) highTier.getAvailableBytes() / highTier.getCapacityBytes();
      if (currentFreeSpace > ServerConfiguration
          .getDouble(PropertyKey.WORKER_MANAGEMENT_TIER_MERGE_PROMOTION_LIMIT)) {
        // Check if there is anything to promote on lower tier.
        Iterator<Long> lowBlocks = mMetadataManager.getBlockIterator()
            .getIterator(intersection.getSecond(), BlockOrder.Reverse);
        while (lowBlocks.hasNext()) {
          if (evictorView.isBlockEvictable(lowBlocks.next())) {
            LOG.debug("Needs merging due for promotion from: {} to {}", intersection.getSecond(),
                intersection.getFirst());
            return true;
          }
        }
      }
    }
    return false;
  }
}
