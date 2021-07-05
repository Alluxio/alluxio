package alluxio.stress.cli;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.rpc.RegisterWorkerParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WorkerHeartbeatBench extends Benchmark<RpcTaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerHeartbeatBench.class);

  @ParametersDelegate
  private RegisterWorkerParameters mParameters = new RegisterWorkerParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private final List<Long> mBlockIds = new ArrayList<>();
  private List<LocationBlockIdListEntry> mLocationBlockIdList;

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

  private RpcTaskResult runRPC() throws Exception {
    // Use a mocked client to save conversion
    LOG.info("Using the MockBlockMasterClient");
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
    RpcTaskResult taskResult = fakeBlockHeartbeat(client, endTime);
    LOG.info("Got {}", taskResult);
    result.merge(taskResult);

    LOG.info("Run finished");
    return result;
  }

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
    // TODO(jiacheng): the preparation includes getting Id and registering the worker
    //  can this be moved to the prepare() step?
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


  @Override
  public void prepare() throws Exception {
    LOG.info("Task ID is {}", mBaseParameters.mId);

    // TODO(jiacheng): ideally this benchmark should be able to generate blocks in a distributed manner
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
    mainInternal(args, new WorkerHeartbeatBench());
  }
}
