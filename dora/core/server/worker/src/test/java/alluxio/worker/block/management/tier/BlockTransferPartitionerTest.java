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

import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.evictor.BlockTransferInfo;
import alluxio.worker.block.management.BlockTransferPartitioner;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class BlockTransferPartitionerTest {
  private BlockTransferPartitioner mPartitioner = new BlockTransferPartitioner();

  @Test
  public void testEmptyList() throws Exception {
    validatePartitions(mPartitioner.partitionTransfers(Collections.emptyList(), 1), 1, 0);
    validatePartitions(mPartitioner.partitionTransfers(Collections.emptyList(), 2), 1, 0);
  }

  @Test
  public void testPartitioning() throws Exception {
    // Test partitioning for when there are 2 distinct locations with "2 transfers each".
    for (List<BlockTransferInfo> transfers : generateTransferLists(4, new int[] {2, 2})) {
      validatePartitions(mPartitioner.partitionTransfers(transfers, 1), 1, 4);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 2), 2, 2, 2);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 3), 2, 2, 2);
    }

    // Test partitioning for when there are 2 distinct locations with "3 and 1" transfers.
    for (List<BlockTransferInfo> transfers : generateTransferLists(4, new int[] {3, 1})) {
      validatePartitions(mPartitioner.partitionTransfers(transfers, 1), 1, 4);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 2), 2, 3, 1);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 3), 2, 3, 1);
    }

    // Test partitioning for when there are 3 distinct locations with "1, 1 and 2" transfers.
    for (List<BlockTransferInfo> transfers : generateTransferLists(4, new int[] {1, 1, 2})) {
      validatePartitions(mPartitioner.partitionTransfers(transfers, 1), 1, 4);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 2), 2, 3, 1);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 3), 3, 1, 1, 2);
    }

    // Test partitioning for when there are 4 distinct locations with "a transfer for each".
    for (List<BlockTransferInfo> transfers : generateTransferLists(4, new int[] {1, 1, 1, 1})) {
      validatePartitions(mPartitioner.partitionTransfers(transfers, 1), 1, 4);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 2), 2, 2, 2);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 3), 3, 2, 1, 1);
      validatePartitions(mPartitioner.partitionTransfers(transfers, 4), 4, 1, 1, 1, 1);
    }
  }

  /**
   * Generates transfer-lists to validate for when SRC/DST or both are
   * allocated(fully identified) locations.
   */
  private List<List<BlockTransferInfo>> generateTransferLists(int count, int[] locDistribution) {
    List<List<BlockTransferInfo>> transfersToValidate = new LinkedList<>();
    transfersToValidate.add(generateTransfers(count, true, false, locDistribution, new int[] {4}));
    transfersToValidate.add(generateTransfers(count, false, true, new int[] {4}, locDistribution));
    transfersToValidate.add(generateTransfers(count, true, true, locDistribution, locDistribution));
    return transfersToValidate;
  }

  /**
   * Generates transfers with controls to specify if src/dst is allocated.
   * It also acceptslocation-group parameter to specify how many transfers will
   * each unique location contain.
   */
  List<BlockTransferInfo> generateTransfers(int count, boolean srcAllocated, boolean dstAllocated,
      int[] srcLocGroups, int[] dstLocGroups) {
    Assert.assertEquals(count, Arrays.stream(srcLocGroups).sum());
    Assert.assertEquals(count, Arrays.stream(dstLocGroups).sum());

    List<BlockTransferInfo> transferInfos = new ArrayList<>(count);
    int[] srcLocGroupsCopy = Arrays.copyOf(srcLocGroups, srcLocGroups.length);
    int[] dstLocGroupsCopy = Arrays.copyOf(dstLocGroups, dstLocGroups.length);
    int srcTierOrdinal = 0;
    int dstTierOrdinal = 0;
    int srcDirIndex = 0;
    int dstDirIndex = 0;
    int currLocationGroupSrc = 0;
    int currLocationGroupDst = 0;
    for (int blockId = 0; blockId < count; blockId++) {
      // Generate the src location.
      BlockStoreLocation srcLoc;
      if (!srcAllocated) {
        srcLoc = BlockStoreLocation.anyDirInTier(Integer.toString(srcTierOrdinal));
      } else {
        srcLoc = new BlockStoreLocation(Integer.toString(srcTierOrdinal), srcDirIndex);
      }

      // Generate the dst location.
      BlockStoreLocation dstLoc;
      if (!dstAllocated) {
        dstLoc = BlockStoreLocation.anyDirInTier(Integer.toString(dstTierOrdinal));
      } else {
        dstLoc = new BlockStoreLocation(Integer.toString(dstTierOrdinal), dstDirIndex);
      }

      // Create the transfer info.
      transferInfos.add(BlockTransferInfo.createMove(srcLoc, blockId, dstLoc));

      // Advance counters for the next location based on requested location groups.
      if (--srcLocGroupsCopy[currLocationGroupSrc] == 0) {
        currLocationGroupSrc++;
        srcTierOrdinal++;
        srcDirIndex++;
      }
      if (--dstLocGroupsCopy[currLocationGroupDst] == 0) {
        currLocationGroupDst++;
        dstTierOrdinal++;
        dstDirIndex++;
      }
    }

    return transferInfos;
  }

  /**
   * Validates given generated partitions.
   *
   * @param transferPartitions partitions list
   * @param expectedPartitionCount number of partitions
   * @param expectedPartitionSizes sizes for each partition
   */
  private void validatePartitions(List<List<BlockTransferInfo>> transferPartitions,
      int expectedPartitionCount, int... expectedPartitionSizes) {
    Assert.assertEquals(expectedPartitionCount, transferPartitions.size());

    for (int i = 0; i < transferPartitions.size(); i++) {
      Assert.assertEquals(expectedPartitionSizes[i], transferPartitions.get(i).size());
    }
  }
}
