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

import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.evictor.BlockTransferInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to partition transfers for concurrent execution.
 */
public class BlockTransferPartitioner {
  private static final Logger LOG = LoggerFactory.getLogger(BlockTransferPartitioner.class);

  /**
   * It greedily partitions given transfers into sub-lists.
   *
   * @param transferInfos list of transfers to partition
   * @param maxPartitionCount max partition count
   * @return transfers partitioned into sub-lists
   */
  public List<List<BlockTransferInfo>> partitionTransfers(List<BlockTransferInfo> transferInfos,
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

    Map<BlockStoreLocation, List<BlockTransferInfo>> transferBuckets = new HashMap<>();
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
      LOG.debug("{}", partitionDbgStr);
    }
    return balancedPartitions;
  }

  /**
   * Used to balance partitions into given bucket count. It greedily tries to achieve each bucket
   * having close count of tasks.
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
      if (!transferInfo.getSrcLocation().isAnyDir()) {
        srcAllocatedCount++;
      }
      if (!transferInfo.getDstLocation().isAnyDir()) {
        dstAllocatedCount++;
      }
      srcLocations.add(transferInfo.getSrcLocation());
      dstLocations.add(transferInfo.getDstLocation());
    }

    // The case for when optimization is not possible.
    if (srcAllocatedCount == 0 && dstAllocatedCount == 0) { // No allocated location.
      // Partitioning not possible. (This is not expected).
      return TransferPartitionKey.NONE;
    }

    // Choose the key by masses.
    if (srcAllocatedCount > dstAllocatedCount) {
      return TransferPartitionKey.SRC;
    } else if (dstAllocatedCount > srcAllocatedCount) {
      return TransferPartitionKey.DST;
    } else { // It's a match. Choose the key by distinction.
      // Partition based on the location that has more distinct sub-locations.
      // This will later be capped by configured parallelism.
      if (srcLocations.size() >= dstLocations.size()) {
        return TransferPartitionKey.SRC;
      } else {
        return TransferPartitionKey.DST;
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
