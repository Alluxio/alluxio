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

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.rpc.BlockMasterBenchParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.FormatUtils;
import alluxio.worker.block.BlockMasterClient;

import alluxio.worker.block.BlockStoreLocation;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A benchmarking tool for the RegisterWorker RPC.
 * The test will generate a specified number of blocks in the master (without associated files).
 * Then it will trigger the specified number of simulated workers to register at once.
 * Each simulated worker will have the specified number of blocks, in order to incur
 * the controlled stress on the master side.
 *
 * Example:
 * Each job worker runs 2 simulated workers, each having 3000 blocks on tier 0 and 10000 blocks
 * on tier 1.
 * $ bin/alluxio runClass alluxio.stress.cli.RegisterWorkerBench --concurrency 2 \
 *   --cluster-limit 1 --tiers "1000,1000,1000;5000,5000"
 */
public class RegisterWorkerBench extends RpcBench<BlockMasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterWorkerBench.class);

  // Constants used for the RPC simulation
  private static final long CAPACITY = 20L * 1024 * 1024 * 1024; // 20GB
  private static final Map<String, Long> CAPACITY_MEM = ImmutableMap.of("MEM", CAPACITY);
  private static final Map<String, Long> USED_MEM_EMPTY = ImmutableMap.of("MEM", 0L);
  private static final List<String> TIER_ALIASES = ImmutableList.of("MEM", "SSD", "HDD");
  private static final Map<String, List<String>> LOST_STORAGE = ImmutableMap.of("MEM", new ArrayList<>());
  private static final List<ConfigProperty> EMPTY_CONFIG = ImmutableList.of();

  @ParametersDelegate
  private BlockMasterBenchParameters mParameters = new BlockMasterBenchParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private List<LocationBlockIdListEntry> mLocationBlockIdList;

  private Deque<Long> mWorkerPool = new ArrayDeque<>();

  @Override
  public void prepare() throws Exception {
    // The task ID is different for local and cluster executions
    // So including that in the log can help associate the log to the run
    LOG.info("Task ID is {}", mBaseParameters.mId);

    // Generate block IDs heuristically
    Map<BlockStoreLocation, List<Long>> blockMap = RpcBenchPreparationUtils.generateBlockIdOnTiers(mParameters.mTiers);
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
    Preconditions.checkState(mWorkerPool.size() == numWorkers, "Expecting %s workers but registered %s",
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

  private void runOnce(alluxio.worker.block.BlockMasterClient client, RpcTaskResult result, long i, long workerId) {
    // TODO(jiacheng): The 1st reported RPC time is always very long, this does
    //  not match with the time recorded by Jaeger.
    //  I suspect it's the time spend in establishing the connection.
    //  The easiest way out is just to ignore the 1st point.
    try {
      Instant s = Instant.now();
      client.register(workerId,
              TIER_ALIASES,
              CAPACITY_MEM,
              USED_MEM_EMPTY,
              // Will use the prepared block list instead of converting on the fly
              // So an empty block list will be used here
              ImmutableMap.of(),
              LOST_STORAGE, // lost storage
              EMPTY_CONFIG); // extra config

      Instant e = Instant.now();
      RpcTaskResult.Point p = new RpcTaskResult.Point(Duration.between(s, e).toNanos());
      result.addPoint(p);
      LOG.info("Iter {} took {}", i, p.mDurationNs);
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

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    Instant startTime = Instant.now();
    Instant endTime = startTime.plus(durationMs, ChronoUnit.MILLIS);
    LOG.info("Start time {}, end time {}", startTime, endTime);

    RpcTaskResult result = new RpcTaskResult();
    result.setBaseParameters(mBaseParameters);
    result.setParameters(mParameters);

    RpcTaskResult taskResult = simulateRegisterWorker(client);
    LOG.info("Received task result {}", taskResult);
    result.merge(taskResult);

    LOG.info("Run finished");
    return result;
  }

  @Override
  public BlockMasterBenchParameters getParameters() {
    return mParameters;
  }
}