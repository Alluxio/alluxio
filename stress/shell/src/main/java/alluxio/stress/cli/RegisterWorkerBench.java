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
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.rpc.BlockMasterBenchParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.FormatUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
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
 * // TODO(Jiacheng): what is the best place for example
 * Simulate new worker registration
 *
 * Examples:
 * For 30s, keep generating new worker registration calls, each one containing 100K block IDs
 * The concurrency you get is concurrency * cluster-limit
 * Each job worker (number controlled by cluster-limit) will spawn a threadpool with size of
 * concurrency, and each thread keeps generating new RPCs.
 * $ bin/alluxio runClass alluxio.stress.cli.RegisterWorkerBench --concurrency 2 \
 *   --cluster-limit 1 --duration 30s --block-count 100000
 *
 */
public class RegisterWorkerBench extends RpcBench<BlockMasterBenchParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterWorkerBench.class);

  @ParametersDelegate
  private BlockMasterBenchParameters mParameters = new BlockMasterBenchParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private List<LocationBlockIdListEntry> mLocationBlockIdList;

  private Deque<Long> mWorkerPool = new ArrayDeque<>();

  @Override
  public void prepare() throws Exception {
    LOG.info("Task ID is {}", mBaseParameters.mId);

    Map<BlockStoreLocation, List<Long>> blockMap = RpcBenchPreparationUtils.generateBlockIdOnTiers(mParameters.mTiers);

    BlockMasterClient client =
            new BlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build());
    mLocationBlockIdList = client.convertBlockListMapToProto(blockMap);

    // Prepare these block IDs concurrently
    LOG.info("Preparing block IDs at the master");
    RpcBenchPreparationUtils.prepareBlocksInMaster(blockMap, getPool(), mParameters.mConcurrency);

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
    String hostname = NetworkAddressUtils.getLocalHostName(500);

    // Each client will simulate only one worker register RPC
    // Because the number of concurrent register RPCs is the variable we want to control
    // And we want these RPCs to invoke at roughly the same time
    runOnce(client, result, i, workerId, hostname);

    return result;
  }

  private void runOnce(alluxio.worker.block.BlockMasterClient client, RpcTaskResult result, long i, long workerId, String hostname) {
    WorkerNetAddress address;
    // TODO(jiacheng): The 1st reported RPC time is always very long, this does
    //  not match with the time recorded by Jaeger.
    //  I suspect it's the time spend in establishing the connection.
    //  The easiest way out is just to ignore the 1st point.
    try {
      List<String> tierAliases = new ArrayList<>();
      tierAliases.add("MEM");
      long cap = 20L * 1024 * 1024 * 1024; // 20GB
      Map<String, Long> capMap = ImmutableMap.of("MEM", cap);
      Map<String, Long> usedMap = ImmutableMap.of("MEM", 0L);

      Instant s = Instant.now();
      client.register(workerId,
              tierAliases,
              capMap,
              usedMap,
              // Will use the prepared block list instead of converting this one
              ImmutableMap.of(),
              ImmutableMap.of("MEM", new ArrayList<>()), // lost storage
              ImmutableList.of()); // extra config

      Instant e = Instant.now();
      RpcTaskResult.Point p = new RpcTaskResult.Point(Duration.between(s, e).toMillis());
      result.addPoint(p);
      LOG.info("Iter {} took {}", i, Duration.between(s, e).toMillis());
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

    // Stop after certain time has elapsed
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