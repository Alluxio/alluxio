package alluxio.client.fs;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RegisterWorkerPResponse;
import alluxio.grpc.StorageList;
import alluxio.master.block.BlockMaster;
import alluxio.proto.meta.Block;
import alluxio.stress.cli.RpcBenchPreparationUtils;
import alluxio.stress.rpc.TierAlias;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.stress.rpc.TierAlias.MEM;
import static org.junit.Assert.assertEquals;

public class RegisterStreamTestUtils {

  public static final Map<String, List<String>> LOST_STORAGE =
          ImmutableMap.of(MEM.toString(), ImmutableList.of());
  public static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();

  private static final Map<Block.BlockLocation, List<Long>> NO_BLOCKS_ON_LOCATION =
          ImmutableMap.of();
  private static final Map<String, StorageList> NO_LOST_STORAGE = ImmutableMap.of();
  private static final int CONCURRENT_CLIENT_COUNT = 20;
  private static final long BLOCK1_ID = 1L;
  private static final long BLOCK1_LENGTH = 49L;
  private static final long BLOCK2_ID = 2L;
  private static final long BLOCK2_LENGTH = 59L;
  private static final Map<String, Long> MEM_CAPACITY = ImmutableMap.of("MEM", 100L);
  private static final Map<String, Long> MEM_USAGE_EMPTY = ImmutableMap.of("MEM", 0L);
  public static final long BLOCK_SIZE = ServerConfiguration.global().getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);

  public static void prepareBlocksOnMaster(BlockMaster blockMaster, Map<BlockStoreLocation, List<Long>> blockMap) throws UnavailableException {
    for (Map.Entry<BlockStoreLocation, List<Long>> entry : blockMap.entrySet()) {
      BlockStoreLocation loc = entry.getKey();
      for (long blockId : entry.getValue()) {
        blockMaster.commitBlockInUFS(blockId, BLOCK_SIZE);
      }
    }
  }

  public static void prepareBLocksOnMaster(BlockMaster blockMaster, Collection<Long> blockIds) throws UnavailableException {
    for (long id : blockIds) {
      blockMaster.commitBlockInUFS(id, BLOCK_SIZE);
    }
  }

  // TODO(jiacheng): this does not prepare the blocks on the master!
  public static List<RegisterWorkerPRequest> generateRegisterStreamForEmptyWorker(long workerId) throws Exception {
    String tierConfig = "";
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
            RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    RegisterStreamer registerStreamer = new RegisterStreamer(null,
            workerId, ImmutableList.of("MEM"), MEM_CAPACITY, MEM_USAGE_EMPTY, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // For an empty worker there is only 1 request
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    assertEquals(1, requestChunks.size());

    return requestChunks;
  }

  private static List<String> getTierAliases(Map<TierAlias, List<Integer>> tierConfig) {
    return tierConfig.keySet().stream().map(TierAlias::toString).collect(Collectors.toList());
  }

  public static List<RegisterWorkerPRequest> generateRegisterStreamForWorker(long workerId) throws Exception {
    // TODO(jiacheng): extract these constants
    String tierConfig = "100,200,300;1000,1500;2000";
    List<String> mTierAliases = getTierAliases(convert(tierConfig));
    Map<String, Long> mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    Map<String, Long> mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
        RpcBenchPreparationUtils.generateBlockIdOnTiers(convert(tierConfig));

    // We just use the RegisterStreamer to generate the batch of requests
    RegisterStreamer registerStreamer = new RegisterStreamer(null,
        workerId, mTierAliases, mCapacityMap, mUsedMap, blockMap, LOST_STORAGE, EMPTY_CONFIG);

    // Get chunks from the RegisterStreamer
    List<RegisterWorkerPRequest> requestChunks = ImmutableList.copyOf(registerStreamer);
    int expectedBatchCount = (int) Math.ceil((100+200+300+1000+1500+2000)/(double)1000);
    assertEquals(expectedBatchCount, requestChunks.size());

    return requestChunks;
  }

  // TODO(jiacheng): improve this
  public static Map<TierAlias, List<Integer>> convert(String tiersConfig) {
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

  // TODO(jiacheng): migrate to ErrorCapturing so the error can be checked on
  static StreamObserver<RegisterWorkerPResponse> getNoopResponseObserver() {
    return new StreamObserver<RegisterWorkerPResponse>() {
      @Override
      public void onNext(RegisterWorkerPResponse response) {
        System.out.format("Response %s%n", response);
      }

      @Override
      public void onError(Throwable t) {
        System.out.format("Error " + t);
        // TODO(jiacheng): use ErrorCapturing one?
      }

      @Override
      public void onCompleted() {
        System.out.println("Completed");
      }
    };
  }

  static StreamObserver<RegisterWorkerPResponse> getErrorCapturingResponseObserver(
          Queue<Throwable> errorQueue
  ) {
    return new StreamObserver<RegisterWorkerPResponse>() {
      @Override
      public void onNext(RegisterWorkerPResponse response) {
        System.out.format("Response %s%n", response);
      }

      @Override
      public void onError(Throwable t) {
        errorQueue.offer(t);
      }

      @Override
      public void onCompleted() {
        System.out.println("Completed");
      }
    };
  }
}
