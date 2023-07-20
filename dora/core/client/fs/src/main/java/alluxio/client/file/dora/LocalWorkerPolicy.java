package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.util.network.NetworkAddressUtils;

import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
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

  /**
   * Finds a local worker from the available workers, matching by hostname.
   * If there are multiple workers matching the client hostname, return the 1st one following
   * the input order.
   */
  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
      String fileId, int count) throws ResourceExhaustedException {
    String userHostname = NetworkAddressUtils.getClientHostName(mConf);
    // TODO(jiacheng): domain socket is not considered here
    // Find the worker matching in hostname
    List<BlockWorkerInfo> results = new ArrayList<>();
    for (BlockWorkerInfo worker : blockWorkerInfos) {
      WorkerNetAddress workerAddr = worker.getNetAddress();
      if (workerAddr == null) {
        continue;
      }
      if (userHostname.equals(workerAddr.getHost())) {
        results.add(worker);
        if (results.size() >= count) {
          break;
        }
      }
    }
    if (results.size() < count) {
      throw new ResourceExhaustedException(String.format(
          "Failed to find a local worker for client hostname %s", userHostname));
    }
    return results;
  }
}
