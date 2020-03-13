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
import alluxio.worker.block.annotator.BlockOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * {@link ManagementTaskProvider} implementation for tier management tasks.
 *
 * It currently creates two types of tasks:
 *  1- TierSwap task for swapping blocks between tiers in order to promote/demote blocks.
 *  2- TierMove task for utilizing speed of higher tiers by moving blocks from below.
 */
public class TierManagementTaskProvider implements ManagementTaskProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TierManagementTaskProvider.class);

  private final BlockStore mBlockStore;
  private final BlockMetadataManager mMetadataManager;
  private final Supplier<BlockMetadataEvictorView> mEvictorViewSupplier;
  private final StoreLoadTracker mLoadTracker;
  private final ExecutorService mExecutor;

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
      case TIER_SWAP:
        return new TierSwapTask(mBlockStore, mMetadataManager, mEvictorViewSupplier.get(),
            mLoadTracker, mExecutor);
      case TIER_MOVE:
        return new TierMoveTask(mBlockStore, mMetadataManager, mEvictorViewSupplier.get(),
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
    boolean tierSwapEnabled =
        ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_SWAP_ENABLED);
    boolean tierMoveEnabled =
        ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_ENABLED);

    // Acquire a recent evictor view.
    BlockMetadataEvictorView evictorView = mEvictorViewSupplier.get();
    // Iterate all tier intersections for deciding whether to run a merge.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      // Check if needs merging due to an overlap.
      if (tierSwapEnabled && mMetadataManager.getBlockIterator().overlaps(intersection.getFirst(),
          intersection.getSecond(), BlockOrder.Natural,
          (blockId) -> !evictorView.isBlockEvictable(blockId))) {
        LOG.debug("Needs block swapping due to an overlap between: {} - {}",
            intersection.getFirst(), intersection.getSecond());
        // Ignore task if load detected.
        if (mLoadTracker.loadDetected(intersection.getFirst(), intersection.getSecond())) {
          LOG.debug("Ignoring overlap between: {} - {} due to user activity.",
              intersection.getFirst(), intersection.getSecond());
        } else {
          return TierManagementTaskType.TIER_SWAP;
        }
      }

      // Check if needs moving blocks from below.
      if (tierMoveEnabled) {
        StorageTier highTier = mMetadataManager.getTier(intersection.getFirst().tierAlias());
        double currentFreeSpace =
            (double) highTier.getAvailableBytes() / highTier.getCapacityBytes();
        if (currentFreeSpace > ServerConfiguration
            .getDouble(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_LIMIT)) {
          // Check if there is anything to move from lower tier.
          Iterator<Long> lowBlocks = mMetadataManager.getBlockIterator()
              .getIterator(intersection.getSecond(), BlockOrder.Reverse);
          while (lowBlocks.hasNext()) {
            if (evictorView.isBlockEvictable(lowBlocks.next())) {
              LOG.debug("Needs merging due to allowed move from {} - {}", intersection.getSecond(),
                  intersection.getFirst());
              // Ignore task if load detected.
              if (mLoadTracker.loadDetected(intersection.getFirst(), intersection.getSecond())) {
                LOG.debug("Ignoring moves from: {} - {} due to user activity.",
                    intersection.getSecond(), intersection.getFirst());
              } else {
                return TierManagementTaskType.TIER_MOVE;
              }
            }
          }
        }
      }
    }
    return TierManagementTaskType.NONE;
  }

  /**
   * Enum for supported tier management tasks.
   */
  enum TierManagementTaskType {
    NONE,
    TIER_SWAP,
    TIER_MOVE
  }
}
