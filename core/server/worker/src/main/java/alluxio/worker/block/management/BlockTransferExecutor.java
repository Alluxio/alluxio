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
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.evictor.BlockTransferInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Used to execute list of {@link BlockTransferInfo} orders concurrently.
 */
public class BlockTransferExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BlockTransferExecutor.class);

  private final ExecutorService mExecutor;
  private final BlockStore mBlockStore;
  private final StoreLoadTracker mLoadTracker;
  private final int mParallelism;

  /**
   * Creates a new instance for executing block transfers.
   *
   * @param executor the executor to use
   * @param blockStore the block store
   * @param loadTracker the load tracker
   * @param parallelism the max concurrent transfers
   */
  public BlockTransferExecutor(ExecutorService executor, BlockStore blockStore,
      StoreLoadTracker loadTracker, int parallelism) {
    mExecutor = executor;
    mBlockStore = blockStore;
    mLoadTracker = loadTracker;
    mParallelism = parallelism;
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
    LOG.debug("Executing transfer list of size: {}. Parallelism: {}",
        transferInfos.size(), mParallelism);
    // Return immediately for an empty transfer list.
    if (transferInfos.isEmpty()) {
      return new BlockOperationResult();
    }
    // Partition executions into sub-lists.
    List<List<BlockTransferInfo>> executionPartitions =
        partitionTransfers(transferInfos, mParallelism);
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
   * Used to partition given transfers into concurrently executable buckets.
   */
  private List<List<BlockTransferInfo>> partitionTransfers(List<BlockTransferInfo> transferInfos,
      int maxPartitionCount) {
    // Bucketing is possible if source or destination has exact location.
    // Those allocated locations will be bucket key[s].
    TransferPartitionKey key = findTransferBucketKey(transferInfos);
    // Can't bucketize transfers.
    if (key == TransferPartitionKey.NONE) {
      LOG.debug("Un-optimizable transfer list encountered.");
      return new ArrayList<List<BlockTransferInfo>>() {
        {
          add(transferInfos);
        }
      };
    }

    Hashtable<BlockStoreLocation, List<BlockTransferInfo>> transferBuckets = new Hashtable<>();
    for (BlockTransferInfo transferInfo : transferInfos) {
      BlockStoreLocation keyLoc;
      switch (key) {
        case SRC:
          keyLoc = transferInfo.getSrcLocation();
          break;
        case DST:
          keyLoc = transferInfo.getDstLocation();
          break;
        default:
          throw new IllegalStateException(
              String.format("Unsupported key type for bucketing transfer infos: %s", key.name()));
      }

      if (!transferBuckets.containsKey(keyLoc)) {
        transferBuckets.put(keyLoc, new LinkedList<>());
      }

      transferBuckets.get(keyLoc).add(transferInfo);
    }

    List<List<BlockTransferInfo>> balancedPartitions = balancePartitions(
        transferBuckets.values().stream().collect(Collectors.toList()), maxPartitionCount);

    // Log partition details.
    if (LOG.isDebugEnabled()) {
      StringBuilder partitionDbgStr = new StringBuilder();
      partitionDbgStr
          .append(String.format("Partitioned %d transfers into %d buckets using key:%s.%n",
              transferInfos.size(), balancedPartitions.size(), key.name()));
      // List each partition content.
      for (int i = 0; i < balancedPartitions.size(); i++) {
        partitionDbgStr.append(String.format("Partition-%d:%n ->%s", i, balancedPartitions.get(i)
            .stream().map(Objects::toString).collect(Collectors.joining("\n ->"))));
      }
      LOG.debug(partitionDbgStr.toString());
    }
    return balancedPartitions;
  }

  /**
   * Used to balance partitions into given bucket count.
   * It greedily tries to achieve each bucket having close count of tasks.
   */
  private List<List<BlockTransferInfo>> balancePartitions(List<List<BlockTransferInfo>> partitions,
      int partitionCount) {
    // Return as is if less than requested bucket count.
    if (partitions.size() <= partitionCount) {
      return partitions;
    }

    // TODO(ggezer): Support partitioning that considers block sizes.
    // Greedily build a balanced partitions by transfer count.
    Collections.sort(partitions, Comparator.comparingInt(List::size));

    // Initialize balanced partitions.
    List<List<BlockTransferInfo>> balancedPartitions = new ArrayList<>(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      balancedPartitions.add(new LinkedList<>());
    }
    // Place partitions into balanced partitions.
    for (List<BlockTransferInfo> partition : partitions) {
      // Find the balanced partition with the least element size.
      int selectedPartitionIdx = -1;
      int selectedPartitionCount = -1;
      for (int i = 0; i < partitionCount; i++) {
        if (balancedPartitions.get(i).size() > selectedPartitionCount) {
          selectedPartitionIdx = i;
          selectedPartitionCount = balancedPartitions.get(i).size();
        }
      }
      balancedPartitions.get(selectedPartitionIdx).addAll(partition);
    }

    return balancedPartitions;
  }

  /**
   * Used to determine right partitioning key by inspecting list of transfers.
   */
  private TransferPartitionKey findTransferBucketKey(List<BlockTransferInfo> transferInfos) {
    int srcAllocatedCount = 0;
    int dstAllocatedCount = 0;
    for (BlockTransferInfo transferInfo : transferInfos) {
      if (transferInfo.getSrcLocation().dir() != BlockStoreLocation.ANY_DIR) {
        srcAllocatedCount++;
      }
      if (transferInfo.getDstLocation().dir() != BlockStoreLocation.ANY_DIR) {
        dstAllocatedCount++;
      }
    }

    if (srcAllocatedCount == dstAllocatedCount) {
      if (srcAllocatedCount == 0) {
        return TransferPartitionKey.NONE;
      } else {
        // Fall-back to SRC partitioning if all are allocated.
        return TransferPartitionKey.SRC;
      }
    } else if (srcAllocatedCount > dstAllocatedCount) {
      return TransferPartitionKey.SRC;
    } else {
      return TransferPartitionKey.DST;
    }
  }

  /**
   * Used as entry point for executing a single transfer partition.
   */
  private BlockOperationResult executeTransferPartition(List<BlockTransferInfo> transferInfos,
      Consumer<Exception> exceptionHandler) {
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

  /**
   * Used to specify how transfers are grouped.
   */
  private enum TransferPartitionKey {
    SRC, DST, NONE
  }
}
