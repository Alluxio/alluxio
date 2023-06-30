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
import alluxio.conf.Configuration;
import alluxio.node.ConsistentHashingNodeProvider;
import alluxio.node.NodeProvider;
import alluxio.node.NodeSelectionHashStrategy;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An impl of WorkerLocationPolicy.
 */
public class WorkerLocationPolicy {
  private static final NodeProvider<BlockWorkerInfo> HASH_PROVIDER =
      ConsistentHashingNodeProvider.create(
          new ArrayList<BlockWorkerInfo>(), 2000,
          workerInfo -> workerInfo.getNetAddress().dumpMainInfo(),
          pair -> isWorkerInfoUpdated(pair.getFirst(), pair.getSecond()));
  private final int mNumVirtualNodes;

  /**
   * Constructs a new {@link WorkerLocationPolicy}.
   *
   * @param numVirtualNodes number of virtual nodes
   */
  public WorkerLocationPolicy(int numVirtualNodes) {
    mNumVirtualNodes = numVirtualNodes;
  }

  /**
   *
   * @param blockWorkerInfos
   * @param fileId
   * @param count
   * @return a list of preferred workers
   */
  public List<BlockWorkerInfo> getPreferredWorkers(List<BlockWorkerInfo> blockWorkerInfos,
                                                   String fileId,
                                                   int count) {
    if (blockWorkerInfos.size() == 0) {
      return ImmutableList.of();
    }
    HASH_PROVIDER.refresh(blockWorkerInfos);
    return HASH_PROVIDER.get(fileId, count);
  }

  private static boolean isWorkerInfoUpdated(Collection<BlockWorkerInfo> workerInfoList,
      Collection<BlockWorkerInfo> anotherWorkerInfoList) {
    if (workerInfoList == anotherWorkerInfoList) {
      return false;
    }
    Set<WorkerNetAddress> workerAddressSet = workerInfoList.stream()
        .map(info -> info.getNetAddress()).collect(Collectors.toSet());
    Set<WorkerNetAddress> anotherWorkerAddressSet = anotherWorkerInfoList.stream()
        .map(info -> info.getNetAddress()).collect(Collectors.toSet());
    return !workerAddressSet.equals(anotherWorkerAddressSet);
  }
}
