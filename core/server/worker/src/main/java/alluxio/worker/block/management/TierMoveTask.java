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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.evictor.BlockTransferInfo;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.annotator.BlockOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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

      // Acquire and execute transfers.
      mTransferExecutor.executeTransferList(getTransferInfos(tierDownIterator, tierUpLocation));
    }
  }

  /**
   * @return list of block transfers
   */
  private List<BlockTransferInfo> getTransferInfos(Iterator<Long> iterator,
      BlockStoreLocation tierUpLocation) {
    // Acquire move range from the configuration.
    // This will limit move operations in single task run.
    final int moveRange = ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_RANGE);

    // Tier for where moves/promotions are targeted.
    StorageTier tierUp = mMetadataManager.getTier(tierUpLocation.tierAlias());
    // Free space limit configured for tier moves.
    double freeSpaceLimit =
        ServerConfiguration.getDouble(PropertyKey.WORKER_MANAGEMENT_TIER_MOVE_LIMIT);

    // List to store transfer infos for selected blocks.
    List<BlockTransferInfo> transferInfos = new LinkedList<>();
    // Projected allocation for selected blocks.
    long bytesToAllocate = 0;
    // Gather blocks from iterator upto configured free space limit.
    while (iterator.hasNext() && transferInfos.size() < moveRange) {
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
        transferInfos.add(
            BlockTransferInfo.createMove(blockMeta.getBlockLocation(), blockId, tierUpLocation));
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to find location of a block:{}. Error: {}", blockId, e);
        continue;
      }
    }
    return transferInfos;
  }
}
