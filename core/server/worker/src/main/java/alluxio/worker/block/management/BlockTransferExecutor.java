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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
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
  private final int mConcurrencyLimit;

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
  }

  /**
   * Executes given list of {@link BlockTransferInfo}s.
   *
   * @param transferInfos the list of transfers
   */
  public void executeTransferList(List<BlockTransferInfo> transferInfos) {
    executeTransferList(transferInfos, null);
  }

  /**
   * Executes given list of {@link BlockTransferInfo}s.
   *
   * @param transferInfos the list of transfers
   * @param exceptionHandler exception handler for when a transfer fails
   */
  public void executeTransferList(List<BlockTransferInfo> transferInfos,
      Consumer<Exception> exceptionHandler) {
    LOG.debug("Executing transfer list of size: {}. Concurrency limit: {}",
        transferInfos.size(), mConcurrencyLimit);
    // Return immediately for an empty transfer list.
    if (transferInfos.isEmpty()) {
      return;
    }
    // Partition executions into sub-lists.
    List<List<BlockTransferInfo>> executionPartitions =
        partitionTransfers(transferInfos, mConcurrencyLimit);
    // Execute to-be-transferred blocks from the plan.
    Collection<Callable<Void>> executionTasks = new LinkedList<>();
    for (List<BlockTransferInfo> executionPartition : executionPartitions) {
      executionTasks.add(() -> {
        // TODO(ggezer): Prevent collisions by locking on locations.
        // Above to-do requires both source and destination locations to be allocated.
        executeTransferPartition(executionPartition, exceptionHandler);
        return null;
      });
    }
    LOG.debug("Executing {} concurrent transfer partitions.", executionTasks.size());
    try {
      mExecutor.invokeAll(executionTasks);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
          .append(String.format("Bucketed %d transfers into %d partitions using key:%s.%n",
              transferInfos.size(), balancedPartitions.size(), key.name()));
      // List each partition content.
      for (int i = 0; i < balancedPartitions.size(); i++) {
        partitionDbgStr.append(String.format("Partition-%d:%n ->%s%n", i, balancedPartitions.get(i)
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
  private List<List<BlockTransferInfo>> balancePartitions(
      List<List<BlockTransferInfo>> transferPartitions, int partitionLimit) {
    // Return as is if less than requested bucket count.
    if (transferPartitions.size() <= partitionLimit) {
      return transferPartitions;
    }

    // TODO(ggezer): Support partitioning that considers block sizes.
    // Greedily build a balanced partitions by transfer count.
    Collections.sort(transferPartitions, Comparator.comparingInt(List::size));

    // Initialize balanced partitions.
    List<List<BlockTransferInfo>> balancedPartitions = new ArrayList<>(partitionLimit);
    for (int i = 0; i < partitionLimit; i++) {
      balancedPartitions.add(new LinkedList<>());
    }
    // Greedily place transfer partitions into balanced partitions.
    for (List<BlockTransferInfo> transferPartition : transferPartitions) {
      // Find the balanced partition with the least element size.
      int selectedPartitionIdx = Integer.MAX_VALUE;
      int selectedPartitionCount = Integer.MAX_VALUE;
      for (int i = 0; i < partitionLimit; i++) {
        if (balancedPartitions.get(i).size() < selectedPartitionCount) {
          selectedPartitionIdx = i;
          selectedPartitionCount = balancedPartitions.get(i).size();
        }
      }
      balancedPartitions.get(selectedPartitionIdx).addAll(transferPartition);
    }

    return balancedPartitions;
  }

  /**
   * Used to determine right partitioning key by inspecting list of transfers.
   */
  private TransferPartitionKey findTransferBucketKey(List<BlockTransferInfo> transferInfos) {
    // How many src/dst locations are fully identified.
    int srcAllocatedCount = 0;
    int dstAllocatedCount = 0;
    // How many unique src/dst locations are seen.
    Set<BlockStoreLocation> srcLocations = new HashSet<>();
    Set<BlockStoreLocation> dstLocations = new HashSet<>();
    // Iterate and process all transfers.
    for (BlockTransferInfo transferInfo : transferInfos) {
      if (transferInfo.getSrcLocation().dir() != BlockStoreLocation.ANY_DIR) {
        srcAllocatedCount++;
      }
      if (transferInfo.getDstLocation().dir() != BlockStoreLocation.ANY_DIR) {
        dstAllocatedCount++;
      }
      srcLocations.add(transferInfo.getSrcLocation());
      dstLocations.add(transferInfo.getDstLocation());
    }

    // Find the desired partitioning key.
    if (srcAllocatedCount == dstAllocatedCount) { // All locations are fully identified.
      if (srcAllocatedCount == 0) {
        // Partitioning not possible. (This is not expected).
        return TransferPartitionKey.NONE;
      } else {
        // Partition based on the location that has more distinct sub-locations.
        // This will later be capped by configured parallelism.
        if (srcLocations.size() >= dstLocations.size()) {
          return TransferPartitionKey.SRC;
        } else {
          return TransferPartitionKey.DST;
        }
      }
    } else if (srcAllocatedCount > dstAllocatedCount) { // Src locations are identified.
      return TransferPartitionKey.SRC;
    } else { // Dst locations are identified.
      return TransferPartitionKey.DST;
    }
  }

  /**
   * Used as entry point for executing a single transfer partition.
   */
  private void executeTransferPartition(List<BlockTransferInfo> transferInfos,
      Consumer<Exception> exceptionHandler) {
    LOG.debug("Executing transfer partition of size {}", transferInfos.size());
    for (BlockTransferInfo transferInfo : transferInfos) {
      try {
        if (mLoadTracker.loadDetected(transferInfo.getSrcLocation(),
            transferInfo.getDstLocation())) {
          LOG.debug("Skipping transfer-order: {} due to user activity.", transferInfo);
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
        if (exceptionHandler != null) {
          exceptionHandler.accept(e);
        }
      }
    }
  }

  /**
   * Used to specify how transfers are grouped.
   */
  private enum TransferPartitionKey {
    SRC, DST, NONE
  }
}
