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

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of WorkerLocationPolicy, where a client will ONLY talk to a local worker.
 *
 * Policy description.
 */
public class RemoteOnlyPolicy implements WorkerLocationPolicy {
  private final AlluxioConfiguration mConf;

  /**
   * Constructs a new {@link RemoteOnlyPolicy}.
   *
   * @param conf the configuration used by the policy
   */
  public RemoteOnlyPolicy(AlluxioConfiguration conf) {
    mConf = conf;
  }

  /**
   * Finds a remote worker from the available workers, matching by hostname.
   */
  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
      String fileId, int count) throws ResourceExhaustedException {
    String userHostname = NetworkAddressUtils.getClientHostName(mConf);
    List<BlockWorkerInfo> results = new ArrayList<>();
    // TODO(tongyu): get result
    if (results.size() < count) {
      throw new ResourceExhaustedException(String.format(
          "Failed to find a local worker for client hostname %s", userHostname));
    }
    return results;
  }
}
