package alluxio.stress.cli;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
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

  @ParametersDelegate
  private WorkerHeartbeatParameters mParameters = new WorkerHeartbeatParameters();

  private final InstancedConfiguration mConf = InstancedConfiguration.defaults();

  private final List<Long> mBlockIds = new ArrayList<>();
  private List<LocationBlockIdListEntry> mLocationBlockIdList;

  private Deque<Long> mWorkerPool = new ArrayDeque<>();

  private static final long CAPACITY = 20L * 1024 * 1024 * 1024; // 20GB
  private static final Map<String, Long> CAPACITY_MEM = ImmutableMap.of("MEM", CAPACITY);
  private static final Map<String, Long> USED_MEM_EMPTY = ImmutableMap.of("MEM", 0L);
  private static final BlockStoreLocation BLOCK_LOCATION_MEM = new BlockStoreLocation("MEM", 0, "MEM");

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

    // Keep sending heartbeats
    long i = 0;
    while (Instant.now().isBefore(endTime)) {
      Instant s = Instant.now();
      try {
        client.heartbeat(workerId,
                CAPACITY_MEM,
                USED_MEM_EMPTY,
                new ArrayList<>(), // no removed blocks
                ImmutableMap.of(BLOCK_LOCATION_MEM, mBlockIds), // added blocks
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
