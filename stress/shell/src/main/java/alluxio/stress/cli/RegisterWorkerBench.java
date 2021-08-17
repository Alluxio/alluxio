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

package alluxio.stress.cli;

import static alluxio.stress.cli.RpcBenchPreparationUtils.CAPACITY;
import static alluxio.stress.cli.RpcBenchPreparationUtils.EMPTY_CONFIG;
import static alluxio.stress.cli.RpcBenchPreparationUtils.LOST_STORAGE;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.rpc.BlockMasterBenchParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.stress.rpc.TierAlias;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A benchmarking tool for the RegisterWorker RPC.
 */
public class RegisterWorkerBench extends RpcBench<BlockMasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterWorkerBench.class);

  @ParametersDelegate
  private BlockMasterBenchParameters mParameters = new BlockMasterBenchParameters();
  private List<String> mTierAliases;
  private Map<String, Long> mCapacityMap;
  private Map<String, Long> mUsedMap;

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private List<LocationBlockIdListEntry> mLocationBlockIdList;

  private Deque<Long> mWorkerPool = new ArrayDeque<>();

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool for the RegisterWorker RPC.",
        "The test will generate a specified number of blocks in the master (without associated "
            + "files). Then it will trigger the specified number of simulated workers to register "
            + "at once.",
        "Each simulated worker will have the specified number of blocks, in order to incur "
            + "the controlled stress on the master side.",
        "",
        "Example:",
        "# 2 job workers will be chosen to run the benchmark",
        "# Each job worker runs 3 simulated workers",
        "# Each simulated worker has 3000 blocks on tier 0 and 10000 on tier 1",
        "# Each simulated worker sends the register RPC once",
        "$ bin/alluxio runClass alluxio.stress.cli.RegisterWorkerBench --concurrency 3 \\",
        "--cluster --cluster-limit 2 --tiers \"1000,1000,1000;5000,5000\"",
        ""
    ));
  }

  @Override
  public void prepare() throws Exception {
    // The task ID is different for local and cluster executions
    // So including that in the log can help associate the log to the run
    LOG.info("Task ID is {}", mBaseParameters.mId);

    mTierAliases = getTierAliases(mParameters.mTiers);
    mCapacityMap = Maps.toMap(mTierAliases, (tier) -> CAPACITY);
    mUsedMap = Maps.toMap(mTierAliases, (tier) -> 0L);
    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap =
        RpcBenchPreparationUtils.generateBlockIdOnTiers(mParameters.mTiers);
    BlockMasterClient client =
            new BlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build());
    mLocationBlockIdList = client.convertBlockListMapToProto(blockMap);

    // Prepare these block IDs concurrently
    LOG.info("Preparing blocks at the master");
    RpcBenchPreparationUtils.prepareBlocksInMaster(blockMap, getPool(), mParameters.mConcurrency);
    LOG.info("Created all blocks at the master");

    // Prepare worker IDs
    int numWorkers = mParameters.mConcurrency;
    mWorkerPool = RpcBenchPreparationUtils.prepareWorkerIds(client, numWorkers);
    Preconditions.checkState(mWorkerPool.size() == numWorkers,
        "Expecting %s workers but registered %s",
        numWorkers, mWorkerPool.size());
    LOG.info("Prepared worker IDs: {}", mWorkerPool);
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new RegisterWorkerBench());
  }

  private RpcTaskResult simulateRegisterWorker(alluxio.worker.block.BlockMasterClient client) {
    RpcTaskResult result = new RpcTaskResult();
    long i = 0;

    if (mWorkerPool == null) {
      result.addError("Worker ID pool is null");
      return result;
    }
    if (mWorkerPool.isEmpty()) {
      result.addError("No more worker IDs for use");
      return result;
    }
    long workerId = mWorkerPool.poll();

    // Each client will simulate only one worker register RPC
    // Because the number of concurrent register RPCs is the variable we want to control
    // And we want these RPCs to invoke at roughly the same time
    runOnce(client, result, i, workerId);

    return result;
  }

  private static List<String> getTierAliases(Map<TierAlias, List<Integer>> tierConfig) {
    LOG.info("Simulate {} tiers with config {}", tierConfig.size(), tierConfig);
    return tierConfig.keySet().stream().map(TierAlias::toString).collect(Collectors.toList());
  }

  private void runOnce(alluxio.worker.block.BlockMasterClient client,
                       RpcTaskResult result, long i, long workerId) {
    try {
      Instant s = Instant.now();
      // TODO(jiacheng): The 1st reported RPC time is always very long, this does
      //  not match with the time recorded by Jaeger.
      //  I suspect it's the time spend in establishing the connection.
      //  The easiest way out is just to ignore the 1st point.
      client.register(workerId,
              mTierAliases,
              mCapacityMap,
              mUsedMap,
              // Will use the prepared block list instead of converting on the fly
              // So an empty block list will be used here
              ImmutableMap.of(),
              LOST_STORAGE, // lost storage
              EMPTY_CONFIG); // extra config

      Instant e = Instant.now();
      RpcTaskResult.Point p = new RpcTaskResult.Point(Duration.between(s, e).toMillis());
      result.addPoint(p);
      LOG.debug("Iter {} took {}ns", i, p.mDurationMs);
    } catch (Exception e) {
      LOG.error("Failed to run iter {}", i, e);
      result.addError(e.getMessage());
    }
  }

  @Override
  public RpcTaskResult runRPC() throws Exception {
    // Use a mocked client to save conversion
    CachingBlockMasterClient client =
            new CachingBlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build(), mLocationBlockIdList);

    RpcTaskResult taskResult = simulateRegisterWorker(client);
    LOG.info("Received task result {}", taskResult);
    LOG.info("Run finished");

    return taskResult;
  }

  @Override
  public BlockMasterBenchParameters getParameters() {
    return mParameters;
  }
}
