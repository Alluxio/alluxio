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
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.annotator.BlockOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

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
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorView the evictor view
   * @param loadTracker the load tracker
   * @param executor the executor
   */
  public TierMoveTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    super(blockStore, metadataManager, evictorView, loadTracker, executor);
  }

  @Override
  public void run() {
    // Iterate each tier intersection and move to upper tier whenever required.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLocation = intersection.getFirst();
      BlockStoreLocation tierDownLocation = intersection.getSecond();

      // Acquire iterator for the tier below.
      Iterator<Long> tierDownIterator =
          mMetadataManager.getBlockIterator().getIterator(tierDownLocation, BlockOrder.Reverse);

      // Generate move tasks for concurrent moving of blocks.
      List<Callable<Void>> moveTasks = new LinkedList<>();
      for (List<Long> moveBucket : generateMoveBuckets(tierDownIterator, tierUpLocation)) {
        moveTasks.add(() -> {
          executeMoveBucket(moveBucket, tierUpLocation, tierDownLocation);
          return null;
        });
      }
      // Execute move tasks concurrently and wait for the results.
      LOG.debug("Invoking {} concurrent move tasks for intersection {}-{}", moveTasks.size(),
          tierUpLocation, tierDownLocation);
      try {
        mExecutor.invokeAll(moveTasks);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  /**
   * Defines a move task that may execute concurrently with others.
   *
   * @param blockIdList the block Ids to move
   * @param tierUpLocation the location up
   * @param tierDownLocation the location down
   */
  private void executeMoveBucket(List<Long> blockIdList, BlockStoreLocation tierUpLocation,
      BlockStoreLocation tierDownLocation) {
    for (Long blockId : blockIdList) {
      // Stop the task if load detected on corresponding tiers.
      if (mLoadTracker.loadDetected(tierUpLocation, tierDownLocation)) {
        // Stop moving if load detected on current tier intersection.
        LOG.warn("Stopping merge task due to user activity.");
        return;
      }

      try {
        mBlockStore.moveBlock(Sessions.createInternalSessionId(), blockId,
            AllocateOptions.forTierMove(tierUpLocation));
      } catch (Exception e) {
        LOG.warn("Move failed during tier-move for block: {}. Error: {}", blockId, e);
      }
    }
  }

  /**
   * Groups given blocks from given iterator into buckets for concurrent execution.
   * It tries to optimize I/O by grouping based on host directory of iterated blocks.
   */
  private List<List<Long>> generateMoveBuckets(Iterator<Long> iterator,
      BlockStoreLocation tierUpLocation) {
    // Acquire move range from the configuration.
    // This will limit move operations in single task run.
    final int moveRange = ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_RANGE);

    // Tier for where moves/promotions are targeted.
    StorageTier tierUp = mMetadataManager.getTier(tierUpLocation.tierAlias());
    // Free space limit configured for tier moves.
    double freeSpaceLimit =
        ServerConfiguration.getDouble(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_LIMIT);

    // List to store <block,location> pairs for selected blocks.
    List<Pair<Long, BlockStoreLocation>> blockLocPairList = new LinkedList<>();
    // Projected allocation for selected blocks.
    long bytesToAllocate = 0;
    // Gather blocks from iterator upto configured free space limit.
    while (iterator.hasNext() && blockLocPairList.size() < moveRange) {
      // Stop moving if reached maximum allowed space on higher tier.
      double currentFreeSpace =
          (double) (tierUp.getAvailableBytes() - bytesToAllocate) / tierUp.getCapacityBytes();
      if (currentFreeSpace <= freeSpaceLimit) {
        break;
      }

      long blockId = iterator.next();
      // Read block info and store it.
      try {
        BlockMeta blockMeta = mEvictorView.getBlockMeta(blockId);
        bytesToAllocate += blockMeta.getBlockSize();
        blockLocPairList.add(new Pair(blockId, blockMeta.getBlockLocation()));
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to find location of a block:{}. Error: {}", blockId, e);
        continue;
      }
    }

    // Process block lists into concurrently executable buckets.
    Map<BlockStoreLocation, List<Long>> moveBucketsMap = new HashMap<>();
    for (Pair<Long, BlockStoreLocation> blockLocPair : blockLocPairList) {
      if (!moveBucketsMap.containsKey(blockLocPair.getSecond())) {
        moveBucketsMap.put(blockLocPair.getSecond(), new LinkedList<>());
      }
      moveBucketsMap.get(blockLocPair.getSecond()).add(blockLocPair.getFirst());
    }

    // How many concurrent tasks will be running.
    int moveConcurrency =
        Math.min(ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_TASK_CONCURRENCY),
            moveBucketsMap.size());

    // Initialize concurrent execution buckets.
    List<List<Long>> concurrentMoveBuckets = new ArrayList<>(moveConcurrency);
    for (int i = 0; i < moveConcurrency; i++) {
      concurrentMoveBuckets.add(new LinkedList<>());
    }

    // Merge buckets as required to satisfy concurrency configuration.
    int concurrencyBucket = 0;
    for (List<Long> swapPairs : moveBucketsMap.values()) {
      concurrencyBucket = (concurrencyBucket + 1) % moveConcurrency;
      concurrentMoveBuckets.get(concurrencyBucket).addAll(swapPairs);
    }
    return concurrentMoveBuckets;
  }
}
