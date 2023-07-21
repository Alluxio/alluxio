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

package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import java.util.ArrayList;
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

  /**
   * Constructs a new {@link LocalWorkerPolicy}.
   *
   * @param conf the configuration used by the policy
   */
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
      // Only a plain string match is performed on hostname
      // If one is IP and the other is hostname, a false negative will be returned
      // Consider PropertyKey.LOCALITY_COMPARE_NODE_IP if that becomes a real request
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
