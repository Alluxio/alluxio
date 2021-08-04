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
import alluxio.stress.rpc.RegisterWorkerParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.FormatUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockMasterClient;

import alluxio.worker.block.BlockStoreLocation;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A tool to simulate RPC
 * */
public class RegisterWorkerBench extends RpcBench<RegisterWorkerParameters> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterWorkerBench.class);

  @ParametersDelegate
  private RegisterWorkerParameters mParameters = new RegisterWorkerParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private final List<Long> mBlockIds = new ArrayList<>();
  private List<LocationBlockIdListEntry> mLocationBlockIdList;
  private AtomicInteger mPortToAssign = new AtomicInteger(50000);

  @Override
  public void prepare() throws Exception {
    LOG.info("Task ID is {}", mBaseParameters.mId);

    Map<BlockStoreLocation, List<Long>> blockMap = GenerateBlockIdUtils.generateBlockIdOnTiers(mParameters.mTiers);

    BlockMasterClient client =
            new BlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build());
    mLocationBlockIdList = client.convertBlockListMapToProto(blockMap);

    // Prepare these block IDs concurrently
    LOG.info("Preparing block IDs at the master");
    GenerateBlockIdUtils.prepareBlocksInMaster(blockMap);
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new RegisterWorkerBench());
  }

  /**
   * Use cases:
   * 1. Simulate new worker registration
   * examples:
   * For 30s, keep generating new worker registration calls, each one containing 100K block IDs
   * The concurrency you get is concurrency * cluster-limit
   * Each job worker (number controlled by cluster-limit) will spawn a threadpool with size of
   * concurrency, and each thread keeps generating new RPCs.
   * bin/alluxio runClass alluxio.stress.cli.RpcBench --concurrency 2 --cluster-limit 1 --duration 30s --rpc registerWorker --block-count 100000
   *
   * 2. Simulate same worker registration
   * bin/alluxio runClass alluxio.stress.cli.RpcBench --concurrency 20 --cluster-limit 1 --duration 30s --rpc registerWorker --block-count 100000 --fake-same-worker
   * With high concurrency, there will be contention over the synchronized(worker) critical sections observed.
   *
   * 3. TODO: Simulate contention over block lock DefaultBlockMaster.mBlockLocks
   * */
  private RpcTaskResult simulateRegisterWorker(alluxio.worker.block.BlockMasterClient client, Instant endTime) {
    RpcTaskResult result = new RpcTaskResult();
    long i = 0;

    WorkerNetAddress address;
    long workerId = -1;
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    LOG.info("Detected local hostname {}", hostname);
    // TODO(jiacheng): need this?
    if (mParameters.mSameWorker) {
      LOG.info("Simulating the same worker registering again.");
      address = new WorkerNetAddress().setHost(hostname)
              .setDataPort(mPortToAssign.getAndIncrement())
              .setRpcPort(mPortToAssign.getAndIncrement());
      try {
        workerId = client.getId(address);
        LOG.info("Got worker ID {}", workerId);
      } catch (Exception e) {
        LOG.error("Failed to run iter {}", i, e);
        result.addError(e.getMessage());
        return result;
      }
    } else {
      LOG.info("Simulating a new worker each time");
    }

    // TODO(jiacheng): default?
    if (mParameters.mOnce) {
      LOG.info("Only run once");
      // If specified to run once, ignore the time stamp and run once
      runOnce(client, result, i, workerId, hostname);
    } else {
      // Otherwise, keep running for as many times as possible
      while (Instant.now().isBefore(endTime)) {
        runOnce(client, result, i, workerId, hostname);
      }
    }

    return result;
  }

  private void runOnce(alluxio.worker.block.BlockMasterClient client, RpcTaskResult result, long i, long workerId, String hostname) {
    WorkerNetAddress address;
    // TODO(jiacheng): The 1st reported RPC time is always very long, this does
    //  not match with the time recorded by Jaeger.
    //  I suspect it's the time spend in establishing the connection.
    //  The easiest way out is just to ignore the 1st point.
    try {
      if (!mParameters.mSameWorker) {
        // If use different worker, get a different address and workerId every time
        address = new WorkerNetAddress().setHost(hostname).setDataPort(mPortToAssign.getAndIncrement()).setRpcPort(mPortToAssign.getAndIncrement());
        workerId = client.getId(address);
        LOG.info("Got new worker ID {}", workerId);
      }

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
    RpcTaskResult taskResult = simulateRegisterWorker(client, endTime);
    LOG.info("Received task result {}", taskResult);
    result.merge(taskResult);

    LOG.info("Run finished");
    return result;
  }

  @Override
  public RegisterWorkerParameters getParameters() {
    return mParameters;
  }
}