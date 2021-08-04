package alluxio.stress.cli;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.resource.ResourcePool;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.rpc.RegisterWorkerParameters;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.stress.rpc.WorkerHeartbeatParameters;
import alluxio.util.FormatUtils;
import alluxio.util.executor.ExecutorServiceFactories;
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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class WorkerHeartbeatBench extends RpcBench<WorkerHeartbeatParameters>{
  private static final Logger LOG = LoggerFactory.getLogger(WorkerHeartbeatBench.class);

  @ParametersDelegate
  private WorkerHeartbeatParameters mParameters = new WorkerHeartbeatParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private final List<Long> mBlockIds = new ArrayList<>();
  private List<LocationBlockIdListEntry> mLocationBlockIdList;

  private Deque<Long> mWorkerPool = new ArrayDeque<>();

  @Override
  public RpcTaskResult runRPC() throws Exception {
    RpcTaskResult result = new RpcTaskResult();
    result.setBaseParameters(mBaseParameters);
    result.setParameters(mParameters);

    if (mWorkerPool == null) {
      result.addError("Worker pool is null");
      throw new NullPointerException("Worker pool is null");
    }
    // Get the worker to use
    long workerId = mWorkerPool.poll();
    LOG.info("Acquired worker ID {}", workerId);

    // Use a mocked client to save conversion
    CachingBlockMasterClient client =
            new CachingBlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build(), mLocationBlockIdList);

    long durationMs = FormatUtils.parseTimeSize(mParameters.mDuration);
    Instant startTime = Instant.now();
    Instant endTime = startTime.plus(durationMs, ChronoUnit.MILLIS);
    LOG.info("Start time {}, end time {}", startTime, endTime);

    // Stop after certain time has elapsed
    RpcTaskResult taskResult = simulateBlockHeartbeat(client, workerId, endTime);
    LOG.info("Got {}", taskResult);
    result.merge(taskResult);

    LOG.info("Run finished");
    return result;
  }

  @Override
  public WorkerHeartbeatParameters getParameters() {
    return mParameters;
  }

  private RpcTaskResult simulateBlockHeartbeat(alluxio.worker.block.BlockMasterClient client, long workerId,
                                               Instant endTime) {
    RpcTaskResult result = new RpcTaskResult();

    // TODO(jiacheng): constant
    long cap = 20L * 1024 * 1024 * 1024; // 20GB
    Map<String, Long> capMap = ImmutableMap.of("MEM", cap);
    Map<String, Long> usedMap = ImmutableMap.of("MEM", 0L);
    BlockStoreLocation mem = new BlockStoreLocation("MEM", 0, "MEM");

    // Keep sending heartbeats
    long i = 0;
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
    // Prepare blocks in the master
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

    // Prepare simulated workers
    int numWorkers = mParameters.mConcurrency;
    for (int i = 0; i < numWorkers; i++) {
      LOG.info("Preparing number {}", i);

      // prepare a worker ID
      int startPort = 9999;
      long workerId;
      String hostname = NetworkAddressUtils.getLocalHostName(500);
      LOG.info("Detected local hostname {}", hostname);
      WorkerNetAddress address = new WorkerNetAddress().setHost(hostname).setDataPort(startPort++).setRpcPort(startPort++);
      workerId = client.getId(address);
      LOG.info("Created worker ID {}", workerId);

      // Register worker
      // TODO(jiacheng): constant
      List<String> tierAliases = new ArrayList<>();
      tierAliases.add("MEM");
      long cap = 20L * 1024 * 1024 * 1024; // 20GB
      Map<String, Long> capMap = ImmutableMap.of("MEM", cap);
      Map<String, Long> usedMap = ImmutableMap.of("MEM", 0L);
      BlockStoreLocation mem = new BlockStoreLocation("MEM", 0, "MEM");
      client.register(workerId,
              tierAliases,
              capMap,
              usedMap,
              ImmutableMap.of(mem, new ArrayList<>()),
              ImmutableMap.of("MEM", new ArrayList<>()), // lost storage
              ImmutableList.of()); // extra config
      LOG.info("Worker {} registered", workerId);

      mWorkerPool.offer(workerId);
    }
    Preconditions.checkState(mWorkerPool.size() == numWorkers, "Expecting %s workers but registered %s",
            numWorkers, mWorkerPool.size());
    LOG.info("All workers registered {}", mWorkerPool);
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new WorkerHeartbeatBench());
  }
}
