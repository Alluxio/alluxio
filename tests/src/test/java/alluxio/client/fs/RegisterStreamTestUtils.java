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

package alluxio.client.fs;

import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.stress.rpc.TierAlias.MEM;
import static org.junit.Assert.assertEquals;

import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

public class RegisterStreamTestUtils {
  static final long MEM_CAPACITY_BYTES = 20_000_000L;
  static final Map<String, List<String>> LOST_STORAGE =
      ImmutableMap.of(MEM.toString(), ImmutableList.of());
  static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();
  static final WorkerNetAddress NET_ADDRESS_1
      = WorkerNetAddress.newBuilder("localhost", 81)
      .setRpcPort(80).setWebPort(82).build();
  static final String TIER_CONFIG = "100,200,300;1000,1500;2000";
  static final int TIER_BLOCK_TOTAL = 100 + 200 + 300 + 1000 + 1500 + 2000;
  static final int BATCH_SIZE = 1000;
  private static final long MEM_USAGE = 20_000L;
  private static final long SSD_USAGE = 500_000L;
  private static final long HDD_USAGE = 1_000_000L;
  static final Map<String, Long> USAGE_MAP = ImmutableMap.of("MEM", MEM_USAGE,
      "SSD", SSD_USAGE, "HDD", HDD_USAGE);
  static final Map<String, Long> CAPACITY_MAP = ImmutableMap.of("MEM", CAPACITY,
      "SSD", CAPACITY, "HDD", CAPACITY);

  static final Map<String, Long> MEM_CAPACITY = ImmutableMap.of("MEM", MEM_CAPACITY_BYTES);
  static final Map<String, Long> MEM_USAGE_EMPTY = ImmutableMap.of("MEM", 0L);

  public static List<RegisterWorkerPRequest> generateRegisterStreamForEmptyWorker(long workerId) {
    String tierConfig = "";
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
        RpcBenchPreparationUtils.generateBlockIdOnTiers(parseTierConfig(tierConfig));

    RegisterStreamer registerStreamer = new RegisterStreamer(null,
        workerId, ImmutableList.of("MEM"), MEM_CAPACITY, MEM_USAGE_EMPTY,
        blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // For an empty worker there is only 1 request
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    assertEquals(1, requestChunks.size());

    return requestChunks;
  }

  static List<String> getTierAliases(Map<TierAlias, List<Integer>> tierConfig) {
    return tierConfig.keySet().stream().map(TierAlias::toString).collect(Collectors.toList());
  }

  public static List<RegisterWorkerPRequest> generateRegisterStreamForWorker(long workerId) {
    List<String> tierAliases = getTierAliases(parseTierConfig(TIER_CONFIG));
    // Generate block IDs heuristically
    Map<TierAlias, List<Integer>> tierConfigMap = parseTierConfig(TIER_CONFIG);
    Map<BlockStoreLocation, List<Long>> blockMap =
        RpcBenchPreparationUtils.generateBlockIdOnTiers(tierConfigMap);

    // We just use the RegisterStreamer to generate the batch of requests
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
        workerId, tierAliases, CAPACITY_MAP, USAGE_MAP, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    int expectedBatchCount = (int) Math.ceil((TIER_BLOCK_TOTAL) / (double) BATCH_SIZE);
    assertEquals(expectedBatchCount, requestChunks.size());

    return requestChunks;
  }

  public static Map<TierAlias, List<Integer>> parseTierConfig(String tiersConfig) {
    String[] tiers = tiersConfig.split(";");
    if (tiers.length == 1 && "".equals(tiers[0])) {
      return ImmutableMap.of();
    }
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

  static StreamObserver<RegisterWorkerPResponse> getErrorCapturingResponseObserver(
      Queue<Throwable> errorQueue) {
    return new StreamObserver<RegisterWorkerPResponse>() {
      @Override
      public void onNext(RegisterWorkerPResponse response) {}

      @Override
      public void onError(Throwable t) {
        errorQueue.offer(t);
      }

      @Override
      public void onCompleted() {}
    };
  }

  static long findFirstBlock(List<RegisterWorkerPRequest> chunks) {
    RegisterWorkerPRequest firstBatch = chunks.get(0);
    LocationBlockIdListEntry entry = firstBatch.getCurrentBlocks(0);
    return entry.getValue().getBlockId(0);
  }
}
