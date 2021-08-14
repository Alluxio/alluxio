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

package alluxio.stress.rpc;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parameters for generating different stress on the BlockMaster.
 */
public class BlockMasterBenchParameters extends RpcBenchParameters {
  @Parameter(names = {"--tiers"}, converter = TierParser.class,
      description = "The number of blocks in each storage dir and tier. "
          + "List tiers in the order of MEM, SSD and HDD separated by semicolon, list dirs "
          + "inside each tier separated by comma. "
          + "Example: \"100,200,300;1000,1500;2000\"")
  public Map<TierAlias, List<Integer>> mTiers;

  private static class TierParser implements IStringConverter<Map<TierAlias, List<Integer>>> {
    @Override
    public Map<TierAlias, List<Integer>> convert(String tiersConfig) {
      String[] tiers = tiersConfig.split(";");
      int length = Math.min(tiers.length, TierAlias.values().length);
      ImmutableMap.Builder<TierAlias, List<Integer>> builder = new ImmutableMap.Builder<>();
      for (int i = 0; i < length; i++) {
        builder.put(
            TierAlias.SORTED.get(i),
            Arrays.stream(tiers[i].split(","))
                .map(Integer::parseInt)
                .collect(Collectors.toList()));
      }
      return builder.build();
    }
  }
}
