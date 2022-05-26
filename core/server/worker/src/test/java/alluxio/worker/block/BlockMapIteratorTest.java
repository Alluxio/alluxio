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

package alluxio.worker.block;

import static org.junit.Assert.assertFalse;

import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.proto.meta.Block;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BlockMapIteratorTest {
  @Test
  public void convertStream() {
    List<Integer> memList = ImmutableList.of(1000, 1000);
    List<Integer> ssdList = ImmutableList.of(10000, 10000);
    List<Integer> hddList = ImmutableList.of(20000, 20000);
    Map<String, List<Integer>> blockConfig =
        ImmutableMap.of("MEM", memList, "SSD", ssdList, "HDD", hddList);

    Map<BlockStoreLocation, List<Long>> blockMap = generateBlockIdOnTiers(blockConfig);

    BlockMapIterator iter = new BlockMapIterator(blockMap);

    while (iter.hasNext()) {
      List<LocationBlockIdListEntry> entries = iter.next();

      // Each batch should not contain duplicate locations
      Set<Block.BlockLocation> locations = new HashSet<>();
      for (LocationBlockIdListEntry e : entries) {
        // No duplicate
        Block.BlockLocation loc = Block.BlockLocation.newBuilder()
            .setTier(e.getKey().getTierAlias())
            .setMediumType(e.getKey().getMediumType())
            .build();
        assertFalse(locations.contains(loc));
        locations.add(loc);
      }
    }
  }

  @Test
  public void convertStreamMergedTiers() {
    List<Integer> memList = ImmutableList.of(100, 100);
    List<Integer> ssdList = ImmutableList.of(101, 22);
    List<Integer> hddList = ImmutableList.of(10, 20);
    Map<String, List<Integer>> blockConfig =
        ImmutableMap.of("MEM", memList, "SSD", ssdList, "HDD", hddList);

    Map<BlockStoreLocation, List<Long>> blockMap = generateBlockIdOnTiers(blockConfig);

    BlockMapIterator iter = new BlockMapIterator(blockMap);

    while (iter.hasNext()) {
      List<LocationBlockIdListEntry> entries = iter.next();

      // Each batch should not contain duplicate locations
      Set<Block.BlockLocation> locations = new HashSet<>();
      for (LocationBlockIdListEntry e : entries) {
        // No duplicate
        Block.BlockLocation loc = Block.BlockLocation.newBuilder()
                .setTier(e.getKey().getTierAlias())
                .setMediumType(e.getKey().getMediumType())
                .build();
        assertFalse(locations.contains(loc));
        locations.add(loc);
      }
    }
  }

  private static Map<BlockStoreLocation, List<Long>> generateBlockIdOnTiers(
          Map<String, List<Integer>> tiersConfig) {
    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();

    long blockIdStart = Long.MAX_VALUE;
    for (Map.Entry<String, List<Integer>> tierConfig : tiersConfig.entrySet()) {
      List<Integer> dirConfigs = tierConfig.getValue();
      for (int i = 0; i < dirConfigs.size(); i++) {
        int dirNumBlocks = dirConfigs.get(i);
        BlockStoreLocation loc = new BlockStoreLocation(tierConfig.getKey(), i);
        List<Long> blockIds = generateDecreasingNumbers(blockIdStart, dirNumBlocks);
        blockMap.put(loc, blockIds);
        blockIdStart -= dirNumBlocks;
      }
    }
    return blockMap;
  }

  private static List<Long> generateDecreasingNumbers(long start, int count) {
    List<Long> list = new ArrayList<>(count);
    for (long i = 0; i < count; i++) {
      list.add(start - i);
    }
    return list;
  }
}
