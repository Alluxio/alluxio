package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A policy where a client will ONLY talk to a local worker.
 *
 * This policy can probably only be used in testing. If client on node A reads path /a,
 * it will only talk to the worker on node A and produce a cache there. If a client on
 * node B reads the same path /a, it will not see the cache on node A.
 *
 * A use case for this policy is a test like StressWorkerBench, where we create clients
 * (with job service) and simulate I/O workload to workers.
 * If all clients talk to their local worker, the test creates balanced stress on each worker
 * and we measure local read performance.
 */
public class LocalWorkerPolicy implements WorkerLocationPolicy {
  private final AlluxioConfiguration mConf;

  public LocalWorkerPolicy(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(
      List<BlockWorkerInfo> blockWorkerInfos, String fileId, int count) {
    String userHostname = NetworkAddressUtils.getClientHostName(mConf);
    // Find the worker matching in hostname
    // TODO(jiacheng): domain socket is not considered here
    BlockWorkerInfo localWorker = null;
    for (BlockWorkerInfo worker : blockWorkerInfos) {
      if (worker.getNetAddress().getHost().equals(userHostname)) {
        localWorker = worker;
        break;
      }
    }
    if (localWorker != null) {
      return ImmutableList.of(localWorker);
    } else {
      return ImmutableList.of();
    }
  }
}
