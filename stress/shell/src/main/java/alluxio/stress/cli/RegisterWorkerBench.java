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
import alluxio.cli.ValidationUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.master.MasterClientContext;
import alluxio.stress.worker.RpcParameters;
import alluxio.stress.worker.RpcTaskResult;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockMasterClient;
import alluxio.stress.worker.IOTaskResult;
import alluxio.stress.worker.UfsIOParameters;
import alluxio.util.executor.ExecutorServiceFactories;

import alluxio.worker.block.BlockStoreLocation;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.UnmodifiableLazyStringList;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A benchmark tool measuring the IO to UFS.
 * */
public class RegisterWorkerBench extends Benchmark<RpcTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterWorkerBench.class);

  @ParametersDelegate
  private RpcParameters mParameters = new RpcParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private final UUID mTaskId = UUID.randomUUID();

  private final List<Long> mBlockIds = new ArrayList<>();

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
      LOG.info("{} futures collected", results.size());
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
    // Generate this may random block IDs
    for (int i = 0; i < mParameters.mBlockCount; i++) {
      long r = ThreadLocalRandom.current().nextLong(0, 1_000_000_000L);
      mBlockIds.add(r);
    }
    LOG.info("Generated {} random block IDs", mBlockIds.size());
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new RegisterWorkerBench());
  }

  private RpcTaskResult runRPC() throws Exception {
    BlockMasterClient client = new BlockMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(mConf))
            .build());

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    Instant startTime = Instant.now();
    Instant endTime = startTime.plus(durationMs, ChronoUnit.MILLIS);
    LOG.info("Start time {}, end time {}", startTime, endTime);

    long i = 0;
    RpcTaskResult result = new RpcTaskResult();
    result.setBaseParameters(mBaseParameters);
    result.setParameters(mParameters);

    // Stop after certain time has elapsed
    int startPort = 9999;
    while (Instant.now().isBefore(endTime)) {
      Instant s = Instant.now();

      // TODO(jiacheng): better host address
      WorkerNetAddress address = new WorkerNetAddress().setHost("localhost").setDataPort(startPort++).setRpcPort(startPort++);
      long workerId = client.getId(address);
      LOG.info("Got worker ID {}", workerId);

      List<String> tierAliases = new ArrayList<>();
      tierAliases.add("MEM");
      long cap = 20L * 1024 * 1024 * 1024; // 20GB
      Map<String, Long> capMap = ImmutableMap.of("MEM", cap);
      Map<String, Long> usedMap = ImmutableMap.of("MEM", 0L);

      client.register(workerId,
              tierAliases,
              capMap,
              usedMap,
              ImmutableMap.of(new BlockStoreLocation("MEM", 0, "MEM"),
                      mBlockIds),
              ImmutableMap.of("MEM", new ArrayList<>()), // lost storage
              ImmutableList.of()); // extra config
      Instant e = Instant.now();
      RpcTaskResult.Point p = new RpcTaskResult.Point(Duration.between(s, e).toMillis());
      result.addPoint(p);
      LOG.info("Iter {} took {}", i, Duration.between(s, e).toMillis());
    }
    LOG.info("Run finished");
    return result;
  }
}
