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

import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.collections.Pair;
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
import alluxio.worker.block.management.StoreLoadTracker;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.annotator.BlockOrder;
import alluxio.worker.block.meta.StorageTierView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Block management task that is used to free up space on reserved spaces of each directory.
 * It's required to guarantee termination of {@link AlignTask}s under variable block size.
 *
 * It essentially does a full cascading evict followed by balancing of each directory.
 */
public class SwapRestoreTask extends AbstractBlockManagementTask {
  private static final Logger LOG = LoggerFactory.getLogger(SwapRestoreTask.class);

  /**
   * Creates a new swap-restore task.
   *
   * @param blockStore the block store
   * @param metadataManager the meta manager
   * @param evictorView the evictor view
   * @param loadTracker the load tracker
   * @param executor the executor
   */
  public SwapRestoreTask(BlockStore blockStore, BlockMetadataManager metadataManager,
      BlockMetadataEvictorView evictorView, StoreLoadTracker loadTracker,
      ExecutorService executor) {
    super(blockStore, metadataManager, evictorView, loadTracker, executor);
  }

  @Override
  public BlockManagementTaskResult run() {
    LOG.debug("Running swap-restore task.");
    // Generate swap-restore plan.
    Pair<List<Long>, List<BlockTransferInfo>> swapRestorePlan = getSwapRestorePlan();
    LOG.debug("Generated swap-restore plan with {} deletions and {} transfers.",
        swapRestorePlan.getFirst().size(), swapRestorePlan.getSecond().size());

    BlockManagementTaskResult result = new BlockManagementTaskResult();
    // Execute to-be-removed blocks from the plan.
    int removalFailCount = 0;
    for (Long blockId : swapRestorePlan.getFirst()) {
      try {
        mBlockStore.removeBlock(Sessions.createInternalSessionId(), blockId);
      } catch (Exception e) {
        LOG.warn("Failed to remove block: {} during swap-restore task.", blockId);
        removalFailCount++;
      }
    }
    result.addOpResults(BlockOperationType.SWAP_RESTORE_REMOVE,
        new BlockOperationResult(swapRestorePlan.getFirst().size(), removalFailCount, 0));

    // Execute to-be-transferred blocks from the plan.
    BlockOperationResult flushResult =
        mTransferExecutor.executeTransferList(swapRestorePlan.getSecond());
    result.addOpResults(BlockOperationType.SWAP_RESTORE_FLUSH, flushResult);

    // Re-balance each tier.
    BlockOperationResult balanceResult =
        mTransferExecutor.executeTransferList(getBalancingTransfersList());
    result.addOpResults(BlockOperationType.SWAP_RESTORE_BALANCE, balanceResult);

    return result;
  }

  /**
   * @return the pair of <blocks-to-remove, blocks-to-transfer> for swap-restore plan
   */
  private Pair<List<Long>, List<BlockTransferInfo>> getSwapRestorePlan() {
    StorageTierAssoc storageTierAssoc = mMetadataManager.getStorageTierAssoc();

    List<Long> blocksToRemove = new LinkedList<>();
    List<BlockTransferInfo> blocksToTransfer = new LinkedList<>();

    long cascadingFromAbove = 0;
    for (StorageTierView tierView : mEvictorView.getTierViews()) {
      int currentTierOrdinal = tierView.getTierViewOrdinal();
      String tierAlias = storageTierAssoc.getAlias(currentTierOrdinal);
      BlockStoreLocation destLocation = BlockStoreLocation.anyDirInTier(tierAlias);
      if (currentTierOrdinal < storageTierAssoc.size() - 1) {
        destLocation =
            BlockStoreLocation.anyDirInTier(storageTierAssoc.getAlias(currentTierOrdinal + 1));
      }
      boolean lastTier = currentTierOrdinal == storageTierAssoc.size() - 1;

      long tierReservedBytes = 0;
      long tierCapacityBytes = 0;
      long tierCommittedBytes = 0;
      for (StorageDirView dirView : tierView.getDirViews()) {
        tierReservedBytes += dirView.getReservedBytes();
        tierCapacityBytes += dirView.getCapacityBytes();
        tierCommittedBytes += dirView.getCommittedBytes();
      }

      long bytesBeyondReserve =
          (tierCommittedBytes + cascadingFromAbove) - (tierCapacityBytes - tierReservedBytes);
      long moveOutBytes = Math.max(0, bytesBeyondReserve);

      // Store for the next tier.
      cascadingFromAbove = moveOutBytes;

      Iterator<Long> tierIterator = mMetadataManager.getBlockIterator()
          .getIterator(BlockStoreLocation.anyDirInTier(tierAlias), BlockOrder.NATURAL);
      while (tierIterator.hasNext() && moveOutBytes > 0) {
        long blockId = tierIterator.next();
        try {
          BlockMeta nextBlockFromTier = mEvictorView.getBlockMeta(blockId);
          if (nextBlockFromTier == null) {
            LOG.debug("Block:{} exist but not available for moving.", blockId);
            continue;
          }
          moveOutBytes -= nextBlockFromTier.getBlockSize();
          if (lastTier) {
            blocksToRemove.add(nextBlockFromTier.getBlockId());
          } else {
            blocksToTransfer.add(BlockTransferInfo.createMove(nextBlockFromTier.getBlockLocation(),
                nextBlockFromTier.getBlockId(), destLocation));
          }
        } catch (BlockDoesNotExistException e) {
          LOG.warn("Failed to find block:{} during cascading calculation.", blockId);
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Generated swap-restore plan with {} deletions and {} transfers.\n"
              + "Block deletions:\n ->{}\nBlock transfers:\n ->{}",
          blocksToRemove.size(), blocksToTransfer.size(),
          blocksToRemove.stream().map(Object::toString).collect(Collectors.joining(",\n ->")),
          blocksToTransfer.stream().map(Object::toString).collect(Collectors.joining(",\n ->")));
    }
    return new Pair<>(blocksToRemove, blocksToTransfer);
  }

  /**
   * @return the list of transfer in order to balance swap-space within tier
   */
  private List<BlockTransferInfo> getBalancingTransfersList() {
    List<BlockTransferInfo> transferInfos = new LinkedList<>();
    for (StorageTierView tierView : mEvictorView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        Iterator<Long> dirBlockIter = mMetadataManager.getBlockIterator().getIterator(
            new BlockStoreLocation(tierView.getTierViewAlias(), dirView.getDirViewIndex()),
            BlockOrder.NATURAL);
        while (dirBlockIter.hasNext() && dirView.getAvailableBytes() < dirView.getReservedBytes()) {
          long blockId = dirBlockIter.next();
          try {
            BlockMeta movingOutBlock = mEvictorView.getBlockMeta(blockId);
            if (movingOutBlock == null) {
              LOG.debug("Block:{} exist but not available for balancing.", blockId);
              continue;
            }
            // Find where to move the block.
            StorageDirView dstDirView = null;
            for (StorageDirView candidateDirView : tierView.getDirViews()) {
              if (candidateDirView.getDirViewIndex() == dirView.getDirViewIndex()) {
                continue;
              }
              if (candidateDirView.getAvailableBytes()
                  - candidateDirView.getReservedBytes() >= movingOutBlock.getBlockSize()) {
                dstDirView = candidateDirView;
                break;
              }
            }

            if (dstDirView == null) {
              LOG.warn("Could not balance swap-restore space for location: {}",
                  dirView.toBlockStoreLocation());
              break;
            }

            // TODO(ggezer): Consider allowing evictions for this move.
            transferInfos.add(BlockTransferInfo.createMove(movingOutBlock.getBlockLocation(),
                blockId, dstDirView.toBlockStoreLocation()));

            // Account for moving-out blocks.
            ((StorageDirEvictorView) dirView).markBlockMoveOut(blockId,
                movingOutBlock.getBlockSize());
            // Account for moving-in blocks.
            ((StorageDirEvictorView) dstDirView).markBlockMoveIn(blockId,
                movingOutBlock.getBlockSize());
          } catch (BlockDoesNotExistException e) {
            LOG.warn("Failed to find metadata for block:{} during swap-restore balancing.",
                blockId);
          }
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated {} balance transfers:\n ->{}", transferInfos.size(),
          transferInfos.stream().map(Object::toString).collect(Collectors.joining(",\n ->")));
    }
    return transferInfos;
  }
}
