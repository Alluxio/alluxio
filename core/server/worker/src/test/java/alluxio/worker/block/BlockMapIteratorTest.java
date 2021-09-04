package alluxio.worker.block;

import alluxio.grpc.LocationBlockIdListEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

      // Validate this
      for (LocationBlockIdListEntry e : entries) {
        System.out.format("<%s, %s>%n", e.getKey(), e.getValue().getBlockIdCount());
      }
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
