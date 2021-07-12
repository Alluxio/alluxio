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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.evictor.BlockTransferInfo;
import alluxio.worker.block.management.AbstractBlockManagementTask;
import alluxio.worker.block.management.BlockManagementTaskResult;
import alluxio.worker.block.management.BlockOperationResult;
import alluxio.worker.block.management.BlockOperationType;
import alluxio.worker.block.management.ManagementTaskCoordinator;
import alluxio.worker.block.management.StoreLoadTracker;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.annotator.BlockOrder;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * A BlockStore management task that is to move blocks to higher tiers.
 * This is to ensure higher tiers are utilized to increase overall performance.
 *
 * A single task may not be enough to complete promotion, so {@link ManagementTaskCoordinator}
 * will keep instantiating new promote tasks until no longer needed.
 */
public class PromoteTask extends AbstractBlockManagementTask {
  private static final Logger LOG = LoggerFactory.getLogger(PromoteTask.class);

  /**
   * Creates a new promote task.
   *
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorView the evictor view
   * @param loadTracker the load tracker
   * @param executor the executor
   */
  public PromoteTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    super(blockStore, metadataManager, evictorView, loadTracker, executor);
  }

  @Override
  public BlockManagementTaskResult run() {
    LOG.debug("Running promote task.");
    BlockManagementTaskResult result = new BlockManagementTaskResult();
    // Iterate each tier intersection and move to upper tier whenever required.
    for (Pair<BlockStoreLocation, BlockStoreLocation> intersection : mMetadataManager
        .getStorageTierAssoc().intersectionList()) {
      BlockStoreLocation tierUpLoc = intersection.getFirst();
      BlockStoreLocation tierDownLoc = intersection.getSecond();

      // Acquire iterator for the tier below.
      Iterator<Long> tierDownIterator =
          mMetadataManager.getBlockIterator().getIterator(tierDownLoc, BlockOrder.REVERSE);

      // Acquire and execute promotion transfers.
      BlockOperationResult tierResult = mTransferExecutor.executeTransferList(
          getTransferInfos(tierDownIterator, tierUpLoc, tierDownLoc));

      result.addOpResults(BlockOperationType.PROMOTE_MOVE, tierResult);
    }
    return result;
  }

  /**
   * @return list of block transfers
   */
  private List<BlockTransferInfo> getTransferInfos(Iterator<Long> iterator,
      BlockStoreLocation tierUpLocation, BlockStoreLocation tierDownLocation) {
    // Acquire promotion range from the configuration.
    // This will limit promotions in single task run.
    final int promoteRange =
        ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_RANGE);

    // Tier for where promotions are going to.
    StorageTier tierUp = mMetadataManager.getTier(tierUpLocation.tierAlias());
    // Get quota for promotions.
    int promotionQuota =
        ServerConfiguration.getInt(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_QUOTA_PERCENT);
    Preconditions.checkArgument(promotionQuota >= 0 && promotionQuota <= 100,
        "Invalid promotion quota percent");
    double quotaRatio = (double) promotionQuota / 100;

    // List to store transfer infos for selected blocks.
    List<BlockTransferInfo> transferInfos = new LinkedList<>();
    // Projected allocation for selected blocks.
    long bytesToAllocate = 0;
    // Gather blocks from iterator upto configured free space limit.
    while (iterator.hasNext() && transferInfos.size() < promoteRange) {
      // Stop moving if reached promotion quota on higher tier.
      double projectedUsedRatio = 1.0
          - ((double) (tierUp.getAvailableBytes() - bytesToAllocate) / tierUp.getCapacityBytes());
      if (projectedUsedRatio >= quotaRatio) {
        break;
      }

      long blockId = iterator.next();
      // Read block info and store it.
      try {
        BlockMeta blockMeta = mEvictorView.getBlockMeta(blockId);
        if (blockMeta == null) {
          LOG.debug("Block:{} exist but not available for promotion.", blockId);
          continue;
        }
        bytesToAllocate += blockMeta.getBlockSize();
        transferInfos.add(
            BlockTransferInfo.createMove(blockMeta.getBlockLocation(), blockId, tierUpLocation));
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed to find location of a block:{}. Error: {}", blockId, e);
        continue;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated {} promotions from {} to {}.\n" + "Promotions transfers:\n ->{}",
          transferInfos.size(), tierDownLocation.tierAlias(), tierUpLocation.tierAlias(),
          transferInfos.stream().map(Objects::toString).collect(Collectors.joining("\n ->")));
    }
    return transferInfos;
  }
}
