package alluxio.stress.cli;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.Command;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.stress.CachingBlockMasterClient;
import alluxio.stress.rpc.RpcTaskResult;
import alluxio.stress.rpc.WorkerHeartbeatParameters;
import alluxio.util.FormatUtils;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Preconditions;
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

public class WorkerHeartbeatBench extends RpcBench<WorkerHeartbeatParameters>{
  private static final Logger LOG = LoggerFactory.getLogger(WorkerHeartbeatBench.class);
  private static final long CAPACITY = 20L * 1024 * 1024 * 1024; // 20GB
  private static final Map<String, Long> CAPACITY_MEM = ImmutableMap.of("MEM", CAPACITY);
  private static final Map<String, Long> USED_MEM_EMPTY = ImmutableMap.of("MEM", 0L);
  private static final BlockStoreLocation BLOCK_LOCATION_MEM = new BlockStoreLocation("MEM", 0, "MEM");

  @ParametersDelegate
  private WorkerHeartbeatParameters mParameters = new WorkerHeartbeatParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();
  // Worker IDs to use in the testing stage
  private Deque<Long> mWorkerPool = new ArrayDeque<>();
  // The prepared RPC contents
  private List<LocationBlockIdListEntry> mLocationBlockIdList;

  @Override
  public RpcTaskResult runRPC() throws Exception {
    RpcTaskResult result = new RpcTaskResult();
    result.setBaseParameters(mBaseParameters);
    result.setParameters(mParameters);

    if (mWorkerPool == null) {
      result.addError("Worker ID pool is null");
      return result;
    }
    // Get the worker to use
    if (mWorkerPool.isEmpty()) {
      result.addError("No more worker IDs for use");
      return result;
    }
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
    LOG.info("Test start time {}, end time {}", startTime, endTime);

    // Stop after certain time has elapsed
    RpcTaskResult taskResult = simulateBlockHeartbeat(client, workerId, endTime);
    LOG.info("Test finished with results: {}", taskResult);
    result.merge(taskResult);
    return result;
  }

  @Override
  public WorkerHeartbeatParameters getParameters() {
    return mParameters;
  }

  private RpcTaskResult simulateBlockHeartbeat(alluxio.worker.block.BlockMasterClient client, long workerId,
                                               Instant endTime) {
    RpcTaskResult result = new RpcTaskResult();

    // Keep sending heartbeats until the expected end time
    long i = 0;
    while (Instant.now().isBefore(endTime)) {
      Instant s = Instant.now();
      try {
        Command cmd = client.heartbeat(workerId,
                CAPACITY_MEM,
                USED_MEM_EMPTY,
                new ArrayList<>(), // no removed blocks
                ImmutableMap.of(BLOCK_LOCATION_MEM, new ArrayList<>()), // added blocks
                ImmutableMap.of(), // lost storage
                new ArrayList<>()); // metrics
        LOG.debug("Received command from heartbeat {}", cmd);
        Instant e = Instant.now();
        Duration d = Duration.between(s, e);
        RpcTaskResult.Point p = new RpcTaskResult.Point(d.toMillis());
        LOG.debug("Iter {} took {}ns", i, d.toNanos());
        result.addPoint(p);
      } catch (Exception e) {
        LOG.error("Failed to run blockHeartbeat {}", i, e);
        result.addError(e.getMessage());
        // Keep trying even when an exception is met
      }
    }

    return result;
  }

  @Override
  public void prepare() throws Exception {
    // The task ID is different for local and cluster executions
    // So including that in the log can help associate the log to the run
    LOG.info("Task ID is {}", mBaseParameters.mId);

    // Prepare block IDs to use for this test
    // We prepare the IDs before test starts so each RPC does not waste time in the conversion
    Map<BlockStoreLocation, List<Long>> blockMap = RpcBenchPreparationUtils.generateBlockIdOnTiers(mParameters.mTiers);
    BlockMasterClient client =
            new BlockMasterClient(MasterClientContext
                    .newBuilder(ClientContext.create(mConf))
                    .build());
    mLocationBlockIdList = client.convertBlockListMapToProto(blockMap);

    // Prepare these block IDs concurrently
    LOG.info("Preparing block IDs at the master");
    RpcBenchPreparationUtils.prepareBlocksInMaster(blockMap, getPool(), mParameters.mConcurrency);

    // Prepare simulated workers
    int numWorkers = mParameters.mConcurrency;
    LOG.info("Register {} simulated workers for the test", numWorkers);
    mWorkerPool = RpcBenchPreparationUtils.prepareWorkerIds(client, numWorkers);
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
