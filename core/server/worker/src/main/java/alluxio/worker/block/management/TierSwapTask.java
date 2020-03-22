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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.annotator.BlockOrder;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorView the evictor view
   * @param loadTracker the load tracker
   * @param executor the executor
   */
  public TierSwapTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    super(blockStore, metadataManager, evictorView, loadTracker, executor);
  }

  @Override
  public void run() {
    // Acquire swap range from the configuration.
    // This will limit swap operations in a single run.
    final int swapRange = ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_SWAP_RANGE);

    // Iterate each tier intersection and avoid overlaps by swapping blocks.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLocation = intersection.getFirst();
      BlockStoreLocation tierDownLocation = intersection.getSecond();

      // Get blocks lists for swapping that will be swapped to eliminate overlap.
      Pair<List<Long>, List<Long>> swapLists = mMetadataManager.getBlockIterator().getSwaps(
          tierUpLocation, BlockOrder.Natural, tierDownLocation, BlockOrder.Reverse, swapRange,
          BlockOrder.Reverse, (blockId) -> !mEvictorView.isBlockEvictable(blockId));

      Preconditions.checkArgument(swapLists.getFirst().size() == swapLists.getSecond().size());
      // Generate task per concurrently executable swap buckets.
      List<Callable<Void>> swapTasks = new LinkedList<>();
      for (List<Pair<Long, Long>> swapBucket : generateSwapBuckets(swapLists)) {
        swapTasks.add(() -> {
          executeSwapBucket(swapBucket, tierUpLocation, tierDownLocation);
          return null;
        });
      }
      // Execute swap-tasks concurrently and wait for the results.
      LOG.debug("Invoking {} concurrent swap tasks for intersection {}-{}", swapTasks.size(),
          tierUpLocation, tierDownLocation);
      try {
        mExecutor.invokeAll(swapTasks);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  /**
   * Defines a swap task that will execute many block-store swaps.
   *
   * @param swapBucket the swap bucket
   * @param locationSrc src location
   * @param locationDst dst location
   */
  private void executeSwapBucket(List<Pair<Long, Long>> swapBucket, BlockStoreLocation locationSrc,
      BlockStoreLocation locationDst) {
    // Execute swaps.
    for (Pair<Long, Long> swap : swapBucket) {
      // Stop the task if load detected on corresponding tiers.
      if (mLoadTracker.loadDetected(locationSrc) || mLoadTracker.loadDetected(locationDst)) {
        // Stop swapping if load detected on current tier intersection.
        LOG.warn("Stopping tier-swap task due to user activity.");
        return;
      }
      // Swap blocks on store.
      try {
        // TODO(ggezer): Implement external allocations for earlier failure detection.
        mBlockStore.moveBlock(Sessions.createInternalSessionId(), swap.getFirst(), locationSrc,
            AllocateOptions.forMove(locationDst).setUseReservedSpace(true));
        mBlockStore.moveBlock(Sessions.createInternalSessionId(), swap.getSecond(), locationDst,
            AllocateOptions.forMove(locationSrc).setUseReservedSpace(true));
      } catch (Exception e) {
        LOG.warn("Swapping blocks {}-{} failed with error: {}", swap.getFirst(), swap.getSecond(),
            e);
      }
    }
  }

  /**
   * Groups given swap blocks into buckets for concurrent execution.
   * It tries to optimize I/O by grouping based on source directory for block.
   */
  private List<List<Pair<Long, Long>>> generateSwapBuckets(
      Pair<List<Long>, List<Long>> swapLists) {
    // Function that is used to map blockId to <blockId,location> pair.
    Function<Long, Pair<Long, BlockStoreLocation>> blockToPairFunc = (blockId) -> {
      try {
        return new Pair(blockId, mEvictorView.getBlockMeta(blockId).getBlockLocation());
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to find location of a block:{}. Error: {}", blockId, e);
        return new Pair(blockId, BlockStoreLocation.anyTier());
      }
    };

    // Generate an augmented block lists with locations.
    List<Pair<Long, BlockStoreLocation>> blockLocPairListSrc =
        swapLists.getFirst().stream().map(blockToPairFunc).collect(Collectors.toList());
    List<Pair<Long, BlockStoreLocation>> blockLocPairListDst =
        swapLists.getSecond().stream().map(blockToPairFunc).collect(Collectors.toList());

    // Sort augmented lists by location.
    // This will help to generate the buckets by a linear sweep.
    Comparator<Pair<Long, BlockStoreLocation>> comparator = (o1, o2) -> {
      BlockStoreLocation loc1 = o1.getSecond();
      BlockStoreLocation loc2 = o2.getSecond();
      int tierComp = loc1.tierAlias().compareTo(loc2.tierAlias());
      if (tierComp != 0) {
        return tierComp;
      } else {
        return loc1.dir() - loc2.dir();
      }
    };
    Collections.sort(blockLocPairListSrc, comparator);
    Collections.sort(blockLocPairListDst, comparator);

    // Process block lists into concurrently executable buckets.
    Map<BlockStoreLocation, List<Pair<Long, Long>>> swapPairsMap = new HashMap<>();
    for (Pair<Long, BlockStoreLocation> blockLocPair : blockLocPairListSrc) {
      if (!swapPairsMap.containsKey(blockLocPair.getSecond())) {
        swapPairsMap.put(blockLocPair.getSecond(), new LinkedList<>());
      }
      swapPairsMap.get(blockLocPair.getSecond())
          .add(new Pair(blockLocPair.getFirst(), blockLocPairListDst.remove(0).getFirst()));
    }

    // How many concurrent swap tasks will be running.
    int swapConcurrency =
        Math.min(ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_TASK_CONCURRENCY),
            swapPairsMap.size());

    // Initialize concurrent execution buckets.
    List<List<Pair<Long, Long>>> concurrentSwapPairs = new ArrayList<>(swapConcurrency);
    for (int i = 0; i < swapConcurrency; i++) {
      concurrentSwapPairs.add(new LinkedList<>());
    }

    // Merge buckets as required to satisfy concurrency configuration.
    int concurrencyBucket = 0;
    for (List<Pair<Long, Long>> swapPairs : swapPairsMap.values()) {
      concurrencyBucket = (concurrencyBucket + 1) % swapConcurrency;
      concurrentSwapPairs.get(concurrencyBucket).addAll(swapPairs);
    }
    return concurrentSwapPairs;
  }
}
