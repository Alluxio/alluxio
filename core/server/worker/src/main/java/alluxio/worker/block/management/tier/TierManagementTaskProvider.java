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

package alluxio.worker.block.management.tier;

import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.management.BlockManagementTask;
import alluxio.worker.block.management.ManagementTaskProvider;
import alluxio.worker.block.management.StoreLoadTracker;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.annotator.BlockOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * {@link ManagementTaskProvider} implementation for tier management tasks.
 *
 * It currently creates three types of tasks:
 *  1- {@link AlignTask} for aligning tiers based on user access pattern.
 *  2- {@link SwapRestoreTask} for when swap task can't run due to reserved space exhaustion.
 *  3- {@link PromoteTask} for utilizing speed of higher tiers by moving blocks from below.
 */
public class TierManagementTaskProvider implements ManagementTaskProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TierManagementTaskProvider.class);

  private final BlockStore mBlockStore;
  private final BlockMetadataManager mMetadataManager;
  private final Supplier<BlockMetadataEvictorView> mEvictorViewSupplier;
  private final StoreLoadTracker mLoadTracker;
  private final ExecutorService mExecutor;

  /** Used to set whether swap-restore task is required. */
  private static boolean sSwapRestoreRequired = false;

  /**
   * Used to set whether swap-restore task is required.
   * It's used by {@link AlignTask} when a swap fails due to insufficient reserved space.
   *
   * @param swapRestoreRequired whether swap-restore task needs to run
   */
  public static void setSwapRestoreRequired(boolean swapRestoreRequired) {
    sSwapRestoreRequired = swapRestoreRequired;
  }

  /**
   * Creates a task provider for tier management functions.
   *
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorViewSupplier the evictor view supplier
   * @param loadTracker the load tracker
   * @param executor the executor
   */
  public TierManagementTaskProvider(BlockStore blockStore, BlockMetadataManager metadataManager,
      Supplier<BlockMetadataEvictorView> evictorViewSupplier, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    mBlockStore = blockStore;
    mMetadataManager = metadataManager;
    mEvictorViewSupplier = evictorViewSupplier;
    mLoadTracker = loadTracker;
    mExecutor = executor;
  }

  @Override
  public BlockManagementTask getTask() {
    switch (findNextTask()) {
      case NONE:
        return null;
      case ALIGN:
        return new AlignTask(mBlockStore, mMetadataManager, mEvictorViewSupplier.get(),
            mLoadTracker, mExecutor);
      case PROMOTE:
        return new PromoteTask(mBlockStore, mMetadataManager, mEvictorViewSupplier.get(),
            mLoadTracker, mExecutor);
      case SWAP_RESTORE:
        return new SwapRestoreTask(mBlockStore, mMetadataManager, mEvictorViewSupplier.get(),
            mLoadTracker, mExecutor);
      default:
        throw new IllegalArgumentException("Unknown task type.");
    }
  }

  /**
   * @return the next tier management task that is required
   */
  private TierManagementTaskType findNextTask() {
    // Fetch the configuration for supported tasks types.
    boolean alignEnabled =
        ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED);
    boolean swapRestoreEnabled =
        ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_SWAP_RESTORE_ENABLED);
    boolean promotionEnabled =
        ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED);

    // Return swap-restore task if marked.
    if (swapRestoreEnabled && sSwapRestoreRequired) {
      setSwapRestoreRequired(false);
      LOG.debug("Swap-restore needed.");
      return TierManagementTaskType.SWAP_RESTORE;
    }

    // Acquire a recent evictor view.
    BlockMetadataEvictorView evictorView = mEvictorViewSupplier.get();

    // Iterate all tier intersections and decide which task to run.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      // Check if the intersection needs alignment.
      if (alignEnabled && !mMetadataManager.getBlockIterator().aligned(intersection.getFirst(),
          intersection.getSecond(), BlockOrder.NATURAL,
          (blockId) -> !evictorView.isBlockEvictable(blockId))) {
        LOG.debug("Alignment needed between: {} - {}", intersection.getFirst().tierAlias(),
            intersection.getSecond().tierAlias());
        return TierManagementTaskType.ALIGN;
      }

      // Check if the intersection allows for promotions.
      if (promotionEnabled) {
        StorageTier highTier = mMetadataManager.getTier(intersection.getFirst().tierAlias());
        // Current used percent on the high tier of intersection.
        double currentUsedRatio =
            1.0 - (double) highTier.getAvailableBytes() / highTier.getCapacityBytes();
        // Configured promotion quota percent for tiers.
        double quotaRatio = (double) ServerConfiguration
            .getInt(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_QUOTA_PERCENT) / 100;
        // Check if the high tier allows for promotions.
        if (currentUsedRatio < quotaRatio) {
          // Check if there is anything to move from lower tier.
          Iterator<Long> lowBlocks = mMetadataManager.getBlockIterator()
              .getIterator(intersection.getSecond(), BlockOrder.REVERSE);
          while (lowBlocks.hasNext()) {
            if (evictorView.isBlockEvictable(lowBlocks.next())) {
              LOG.debug("Promotions needed from {} to {}", intersection.getSecond().tierAlias(),
                  intersection.getFirst().tierAlias());
              return TierManagementTaskType.PROMOTE;
            }
          }
        }
      }
    }

    // No tier management task is required/allowed.
    return TierManagementTaskType.NONE;
  }

  /**
   * Supported tier management tasks.
   */
  enum TierManagementTaskType {
    NONE,
    ALIGN,
    PROMOTE,
    SWAP_RESTORE
  }
}
