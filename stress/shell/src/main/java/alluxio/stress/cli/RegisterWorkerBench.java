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
import alluxio.stress.rpc.RpcParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.FormatUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.util.executor.ExecutorServiceFactories;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A tool to simulate RPC
 * */
public class RegisterWorkerBench extends Benchmark<RpcTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterWorkerBench.class);

  @ParametersDelegate
  private RpcParameters mParameters = new RpcParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private final List<Long> mBlockIds = new ArrayList<>();
  private List<LocationBlockIdListEntry> mLocationBlockIdList;
  private AtomicInteger mPortToAssign = new AtomicInteger(50000);

  @Override
  public RpcTaskResult runLocal() throws Exception {
    LOG.debug("Running locally with {} threads", mParameters.mConcurrency);
    ExecutorService pool = null;
    List<CompletableFuture<RpcTaskResult>> futures = new ArrayList<>();
    try {
      pool = ExecutorServiceFactories.fixedThreadPool("rpc-thread", mParameters.mConcurrency)
              .create();
      for (int i = 0; i < mParameters.mConcurrency; i++) {
        CompletableFuture<RpcTaskResult> future = CompletableFuture.supplyAsync(() -> {
          RpcTaskResult threadResult = new RpcTaskResult();
          threadResult.setBaseParameters(mBaseParameters);
          threadResult.setParameters(mParameters);
          try {
            RpcTaskResult r = runRPC();
            threadResult.setPoints(r.getPoints());
            threadResult.setErrors(r.getErrors());
            return threadResult;
          } catch (Exception e) {
            LOG.error("Failed to execute RPC", e);
            threadResult.addError(e.getMessage());
            return threadResult;
          }
        }, pool);
        futures.add(future);
      }
      LOG.info("{} jobs submitted", futures.size());

      // Collect the result
      CompletableFuture[] cfs = futures.toArray(new CompletableFuture[0]);
      List<RpcTaskResult> results = CompletableFuture.allOf(cfs)
              .thenApply(f -> futures.stream()
                      .map(CompletableFuture::join)
                      .collect(Collectors.toList())
              ).get();
      LOG.info("{} futures collected: {}", results.size(),
              results.size() > 0 ? results.get(0) : "[]");
      return RpcTaskResult.reduceList(results);
    } catch (Exception e) {
      LOG.error("Failed to execute RPC in pool", e);
      RpcTaskResult result = new RpcTaskResult();
      result.setBaseParameters(mBaseParameters);
      result.setParameters(mParameters);
      result.addError(e.getMessage());
      return result;
    } finally {
      if (pool != null) {
        pool.shutdownNow();
        pool.awaitTermination(30, TimeUnit.SECONDS);
      }
    }
  }

  @Override
  public void prepare() {
    LOG.info("Task ID is {}", mBaseParameters.mId);

    // TODO(jiacheng): how to support running --local which does not have task id?
    // Calucalte the start ID with the worker ID
//    String taskId = mBaseParameters.mId;
//    String workerId = (taskId.split("-"))[1];
//    LOG.info("Found workerId {}", workerId);
//    long startId = (Long.valueOf(workerId).longValue()) << 30;
//    LOG.info("Start block id is {}", startId);

    Map<BlockStoreLocation, List<Long>> blockMap = RpcBenchUtils.generateBlockIdOnTiers(mParameters.mTiers);

    BlockMasterClient client =
            new BlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build());
    mLocationBlockIdList = client.convertBlockListMapToProto(blockMap);
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
  private RpcTaskResult fakeRegisterWorker(alluxio.worker.block.BlockMasterClient client, Instant endTime) {
    RpcTaskResult result = new RpcTaskResult();

    // Stop after certain time has elapsed
    int startPort = 9999;
    long i = 0;

    WorkerNetAddress address;
    long workerId = -1;
    String hostname = NetworkAddressUtils.getLocalHostName(500);
    LOG.info("Detected local hostname {}", hostname);
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
    WorkerNetAddress address;// TODO(jiacheng): The 1st reported RPC time is always very long, this does
    // not match with the time recorded by Jaeger.
    // I suspect it's the time spend in establishing the connection.
    // The easiest way out is just to ignore the 1st point.
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

  // TODO(jiacheng): move this into another class
  private RpcTaskResult fakeBlockHeartbeat(alluxio.worker.block.BlockMasterClient client, Instant endTime) {
    RpcTaskResult result = new RpcTaskResult();
    // prepare a worker ID
    int startPort = 9999;
    long workerId = -1;
    try {
      String hostname = NetworkAddressUtils.getLocalHostName(500);
      LOG.info("Detected local hostname {}", hostname);
      WorkerNetAddress address = new WorkerNetAddress().setHost(hostname).setDataPort(startPort++).setRpcPort(startPort++);
      workerId = client.getId(address);
      LOG.info("Got worker ID {}", workerId);
    } catch (Exception e) {
      LOG.error("Failed to prepare worker ID", e);
      result.addError(e.getMessage());
      return result;
    }

    // Register worker
    List<String> tierAliases = new ArrayList<>();
    tierAliases.add("MEM");
    long cap = 20L * 1024 * 1024 * 1024; // 20GB
    Map<String, Long> capMap = ImmutableMap.of("MEM", cap);
    Map<String, Long> usedMap = ImmutableMap.of("MEM", 0L);
    BlockStoreLocation mem = new BlockStoreLocation("MEM", 0, "MEM");
    try {
      client.register(workerId,
              tierAliases,
              capMap,
              usedMap,
              ImmutableMap.of(mem, new ArrayList<>()),
              ImmutableMap.of("MEM", new ArrayList<>()), // lost storage
              ImmutableList.of()); // extra config
    } catch (Exception e) {
      LOG.error("Failed to register worker", e);
      result.addError(e.getMessage());
      return result;
    }

    // Keep sending heartbeats
    long i = 0;
    // TODO(jiacheng): wasted a lot of time on the preparation
    while (Instant.now().isBefore(endTime)) {
      Instant s = Instant.now();
      try {
        client.heartbeat(workerId,
                capMap,
                usedMap,
                new ArrayList<>(), // no removed blocks
                ImmutableMap.of(mem, mBlockIds), // added blocks
                ImmutableMap.of(), // lost storage
                new ArrayList<>()); // metrics
        Instant e = Instant.now();
        RpcTaskResult.Point p = new RpcTaskResult.Point(Duration.between(s, e).toMillis());
        result.addPoint(p);
        LOG.info("Iter {} took {}", i, Duration.between(s, e).toMillis());
      } catch (Exception e) {
        LOG.error("Failed to run blockHeartbeat {}", i, e);
        result.addError(e.getMessage());
      }
    }

    return result;
  }

  private RpcTaskResult runRPC() throws Exception {
    // Use a mocked client to save conversion
    LOG.info("Using the MockBlockMasterClient");
    MockBlockMasterClient client =
            new MockBlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build(), mLocationBlockIdList);

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    Instant startTime = Instant.now();
    Instant endTime = startTime.plus(durationMs, ChronoUnit.MILLIS);
    LOG.info("Start time {}, end time {}", startTime, endTime);

    RpcTaskResult result = new RpcTaskResult();
    result.setBaseParameters(mBaseParameters);
    result.setParameters(mParameters);

    String rpcName = mParameters.mRpc;
    Supplier<RpcTaskResult> rpcFunc = null;
    switch (rpcName) {
      case "registerWorker":
        rpcFunc = () -> fakeRegisterWorker(client, endTime);
        break;
      case "blockHeartbeat":
        rpcFunc = () -> fakeBlockHeartbeat(client, endTime);
        break;
      default:
        throw new UnsupportedOperationException(String.format("RPC %s is not supported", rpcName));
    }

    // Stop after certain time has elapsed
    RpcTaskResult taskResult = rpcFunc.get();
    LOG.info("Got {}", taskResult);
    result.merge(taskResult);

    LOG.info("Run finished");
    return result;
  }

  /**
   * Use this class to avoid map-list conversion at the client side
   * So if you are running all processes on the same machine,
   * the client side work affect the performance less.
   * */
  public static class MockBlockMasterClient extends BlockMasterClient {
    private List<LocationBlockIdListEntry> mLocationBlockIdList;

    /**
     * Creates a new instance of {@link BlockMasterClient} for the worker.
     *
     * @param conf master client configuration
     */
    public MockBlockMasterClient(MasterClientContext conf, List<LocationBlockIdListEntry> locationBlockIdList) {
      super(conf);
      LOG.info("Init MockBlockMasterClient");
      mLocationBlockIdList = locationBlockIdList;
    }

    @Override
    public List<LocationBlockIdListEntry> convertBlockListMapToProto(
            Map<BlockStoreLocation, List<Long>> blockListOnLocation) {
      LOG.info("Using the prepared mLocationBlockIdList");
      return mLocationBlockIdList;
    }
  }
}