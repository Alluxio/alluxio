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
import alluxio.worker.block.AllocateOptions;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.evictor.BlockTransferInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Used to execute list of {@link BlockTransferInfo} orders concurrently.
 */
public class BlockTransferExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BlockTransferExecutor.class);

  private final ExecutorService mExecutor;
  private final BlockStore mBlockStore;
  private final StoreLoadTracker mLoadTracker;
  private final int mConcurrencyLimit;
  private final BlockTransferPartitioner mPartitioner;

  /**
   * Creates a new instance for executing block transfers.
   *
   * @param executor the executor to use
   * @param blockStore the block store
   * @param loadTracker the load tracker
   * @param concurrencyLimit the max concurrent transfers
   */
  public BlockTransferExecutor(ExecutorService executor, BlockStore blockStore,
      StoreLoadTracker loadTracker, int concurrencyLimit) {
    mExecutor = executor;
    mBlockStore = blockStore;
    mLoadTracker = loadTracker;
    mConcurrencyLimit = concurrencyLimit;
    mPartitioner = new BlockTransferPartitioner();
  }

  /**
   * Executes given list of {@link BlockTransferInfo}s.
   *
   * @param transferInfos the list of transfers
   * @return the result of transfers
   */
  public BlockOperationResult executeTransferList(List<BlockTransferInfo> transferInfos) {
    return executeTransferList(transferInfos, null);
  }

  /**
   * Executes given list of {@link BlockTransferInfo}s.
   *
   * @param transferInfos the list of transfers
   * @param exceptionHandler exception handler for when a transfer fails
   * @return the result of transfers
   */
  public BlockOperationResult executeTransferList(List<BlockTransferInfo> transferInfos,
      Consumer<Exception> exceptionHandler) {
    LOG.debug("Executing transfer list of size: {}. Concurrency limit: {}",
        transferInfos.size(), mConcurrencyLimit);
    // Return immediately for an empty transfer list.
    if (transferInfos.isEmpty()) {
      return new BlockOperationResult();
    }
    // Partition executions into sub-lists.
    List<List<BlockTransferInfo>> executionPartitions =
        mPartitioner.partitionTransfers(transferInfos, mConcurrencyLimit);
    // Counters for ops/failures/backoffs.
    AtomicInteger opCount = new AtomicInteger(0);
    AtomicInteger failCount = new AtomicInteger(0);
    AtomicInteger backOffCount = new AtomicInteger(0);
    // Execute to-be-transferred blocks from the plan.
    Collection<Callable<Void>> executionTasks = new LinkedList<>();
    for (List<BlockTransferInfo> executionPartition : executionPartitions) {
      executionTasks.add(() -> {
        // TODO(ggezer): Prevent collisions by locking on locations.
        // Above to-do requires both source and destination locations to be allocated.
        BlockOperationResult res = executeTransferPartition(executionPartition, exceptionHandler);
        // Accumulate partition results.
        opCount.addAndGet(res.opCount());
        failCount.addAndGet(res.failCount());
        backOffCount.addAndGet(res.backOffCount());
        return null;
      });
    }
    LOG.debug("Executing {} concurrent transfer partitions.", executionTasks.size());
    try {
      mExecutor.invokeAll(executionTasks);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    return new BlockOperationResult(opCount.get(), failCount.get(), backOffCount.get());
  }

  /**
   * Used as entry point for executing a single transfer partition.
   */
  private BlockOperationResult executeTransferPartition(List<BlockTransferInfo> transferInfos,
      Consumer<Exception> exceptionHandler) {
    LOG.debug("Executing transfer partition of size {}", transferInfos.size());
    // Counters for failure and back-offs.
    int failCount = 0;
    int backOffCount = 0;
    // Execute transfers in order.
    for (BlockTransferInfo transferInfo : transferInfos) {
      try {
        if (mLoadTracker.loadDetected(transferInfo.getSrcLocation(),
            transferInfo.getDstLocation())) {
          LOG.debug("Skipping transfer-order: {} due to user activity.", transferInfo);
          backOffCount++;
          continue;
        }

        boolean useReservedSpace = transferInfo.isSwap();

        mBlockStore.moveBlock(Sessions.createInternalSessionId(), transferInfo.getSrcBlockId(),
            AllocateOptions.forTierMove(transferInfo.getDstLocation())
                .setUseReservedSpace(useReservedSpace));
        if (transferInfo.isSwap()) {
          // TODO(ggezer): Implement external allocations to guarantee a swap.
          mBlockStore.moveBlock(Sessions.createInternalSessionId(), transferInfo.getDstBlockId(),
              AllocateOptions.forTierMove(transferInfo.getSrcLocation())
                  .setUseReservedSpace(useReservedSpace));
        }
      } catch (Exception e) {
        LOG.warn("Transfer-order: {} failed. {}. ", transferInfo, e);
        failCount++;
        if (exceptionHandler != null) {
          exceptionHandler.accept(e);
        }
      }
    }

    return new BlockOperationResult(transferInfos.size(), failCount, backOffCount);
  }
}
