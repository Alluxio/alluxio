package alluxio.worker.block;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BlockMapIteratorTest {
  @Test
  public void convertStream() {
    // TODO(jiacheng)
    BlockStoreLocation memTier = new BlockStoreLocation("MEM", 0);
    BlockStoreLocation ssdTier = new BlockStoreLocation("SSD", 0);
    BlockStoreLocation hddTier = new BlockStoreLocation("HDD", 0);
    List<Integer> memList = ImmutableList.of(1000,1000);
    List<Integer> ssdList = ImmutableList.of(10000,10000);
    List<Integer> hddList = ImmutableList.of(20000,20000);
    Map<String, List<Integer>> blockConfig = ImmutableMap.of("MEM", memList, "SSD", ssdList, "HDD", hddList);

    Map<BlockStoreLocation, List<Long>> blockMap = generateBlockIdOnTiers(blockConfig);

    BlockMapIterator iter = new BlockMapIterator(blockMap);

    while (iter.hasNext()) {
      List<LocationBlockIdListEntry> entries = iter.next();

      // Each batch should not contain duplicate locations
      Set<Block.BlockLocation> locations = new HashSet<>();
      for (LocationBlockIdListEntry e : entries) {
        System.out.format("<%s, %s>%n", e.getKey(), e.getValue().getBlockIdCount());
        // No duplicate
        Block.BlockLocation loc = Block.BlockLocation.newBuilder()
                .setTier(e.getKey().getTierAlias())
                .setMediumType(e.getKey().getMediumType())
                .build();
        assertFalse(locations.contains(loc));
        locations.add(loc);
      }
      System.out.println("This batch contains " + locations.size() +" locations: " + locations);
    }
  }


  @Test
  public void convertStreamMergedTiers() {
    // TODO(jiacheng)
    BlockStoreLocation memTier = new BlockStoreLocation("MEM", 0);
    BlockStoreLocation ssdTier = new BlockStoreLocation("SSD", 0);
    BlockStoreLocation hddTier = new BlockStoreLocation("HDD", 0);
    List<Integer> memList = ImmutableList.of(100,100);
    List<Integer> ssdList = ImmutableList.of(101,22);
    List<Integer> hddList = ImmutableList.of(10,20);
    Map<String, List<Integer>> blockConfig = ImmutableMap.of("MEM", memList, "SSD", ssdList, "HDD", hddList);

    Map<BlockStoreLocation, List<Long>> blockMap = generateBlockIdOnTiers(blockConfig);

    BlockMapIterator iter = new BlockMapIterator(blockMap);

    while (iter.hasNext()) {
      List<LocationBlockIdListEntry> entries = iter.next();

      // Each batch should not contain duplicate locations
      Set<Block.BlockLocation> locations = new HashSet<>();
      for (LocationBlockIdListEntry e : entries) {
        System.out.format("<%s, %s>%n", e.getKey(), e.getValue().getBlockIdCount());
        // No duplicate
        Block.BlockLocation loc = Block.BlockLocation.newBuilder()
                .setTier(e.getKey().getTierAlias())
                .setMediumType(e.getKey().getMediumType())
                .build();
        assertFalse(locations.contains(loc));
        locations.add(loc);
      }
      System.out.println("This batch contains " + locations.size() +" locations: " + locations);
    }
  }

  static Map<BlockStoreLocation, List<Long>> generateBlockIdOnTiers(
          Map<String, List<Integer>> tiersConfig) {
    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();

    long blockIdStart = Long.MAX_VALUE;
    for (Map.Entry<String, List<Integer>> tierConfig : tiersConfig.entrySet()) {
      List<Integer> dirConfigs = tierConfig.getValue();
      for (int i = 0; i < dirConfigs.size(); i++) {
        int dirNumBlocks = dirConfigs.get(i);
        System.out.format("Found dir on tier %s with %s blocks%n", tierConfig.getKey(), dirNumBlocks);
        BlockStoreLocation loc = new BlockStoreLocation(tierConfig.getKey().toString(), i);
        List<Long> blockIds = generateDecreasingNumbers(blockIdStart, dirNumBlocks);
        blockMap.put(loc, blockIds);
        blockIdStart -= dirNumBlocks;
      }
    }
    return blockMap;
  }

  private static List<Long> generateDecreasingNumbers(long start, int count) {
    System.out.format("Generating block Ids (%s, %s]%n", start - count, start);
    List<Long> list = new ArrayList<>(count);
    for (long i = 0; i < count; i++) {
      list.add(start - i);
    }
    return list;
  }
}
